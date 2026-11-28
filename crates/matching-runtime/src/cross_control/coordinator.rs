//! 在持久范围栅栏下向显式目标派发子命令。
use super::*;
use crate::{FormalError, FormalOutcome, RouteState, ShardHandle};
use matching_domain::{
    ApplyGlobalControlFenceShard, ApplyUserControlFenceShard, ClusterId, Command, CommandEnvelope,
    UserFenceAction,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::{
    collections::BTreeSet,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

/// 管理线程上的跨分片协调器。重启时用同一控制存储和完整部署拓扑重建。
pub struct CrossShardCoordinator {
    /// 只反映本协调者实际观察到的派发停滞，不参与提交决策。
    telemetry: Arc<ControlTelemetry>,
    /// 持有本代协调者排他写权的存储。
    authority: Arc<dyn ControlAuthority>,
    /// 所有分片所属的业务集群。
    cluster: ClusterId,
    /// 部署时配置的全部分片，包含当前离线的分片。
    members: Vec<ShardId>,
    /// 各目标最近登记的持久分片句柄。派发时使用句柄的主节点纪元并等待实际回执。
    endpoints: RwLock<BTreeMap<ShardId, ShardHandle>>,
    /// 每次等待分片回执的最大时长。
    timeout: Duration,
}

impl CrossShardCoordinator {
    /// 由完整可信拓扑构造，调用方负责取得并续持存储租约。分片掉线不改变目标集合。
    pub fn new(
        authority: Arc<dyn ControlAuthority>,
        cluster: ClusterId,
        members: &[ShardId],
        timeout: Duration,
    ) -> io::Result<Self> {
        let ids: BTreeSet<_> = members.iter().copied().collect();
        if cluster != authority.cluster_id()
            || ids.is_empty()
            || ids.len() != members.len()
            || ids.len() > 4096
            || timeout.is_zero()
        {
            return Err(invalid("控制分片拓扑或超时无效"));
        }
        Ok(Self {
            telemetry: Arc::default(),
            authority,
            cluster,
            members: ids.into_iter().collect(),
            endpoints: RwLock::new(BTreeMap::new()),
            timeout,
        })
    }

    /// 返回供控制面监控登记的进程内观测值。
    pub fn telemetry(&self) -> Arc<ControlTelemetry> {
        Arc::clone(&self.telemetry)
    }

    /// 安装当前主节点的持久入口。接任后的句柄可替换同一目标的失效入口。
    pub fn attach(&self, shard: ShardHandle) -> io::Result<()> {
        let id = shard.shard_id();
        if shard.cluster_id() != self.cluster
            || self.members.binary_search(&id).is_err()
            || !shard.has_durable_formal()
            || shard.control_primary_epoch() == 0
            || shard.control_namespace().as_deref() != Some(self.authority.namespace().as_str())
        {
            return Err(invalid("控制目标身份、准入权威或持久能力不匹配"));
        }
        let table = shard
            .routes()
            .map_err(|_| io::Error::other("无法读取分片路由"))?;
        for route in table.routes() {
            let known = |id| self.members.binary_search(&id).is_ok();
            if !known(route.shard_id)
                || matches!(route.state,RouteState::Migrating {source,target} if !known(source) || !known(target))
            {
                return Err(invalid("控制拓扑遗漏已登记的市场分片"));
            }
        }
        self.endpoints
            .write()
            .map_err(|_| invalid("控制分片句柄锁损坏"))?
            .insert(id, shard);
        Ok(())
    }

    /// 持久登记完整目标和意图，可用原父命令重试。
    pub fn prepare(&self, parent: CommandId, kind: ControlKind) -> io::Result<ControlOperation> {
        self.authority.prepare(parent, kind, &self.members)
    }

    /// 在尚可证明没有子命令在途时，以原子比较终止操作。
    pub fn abort(&self, parent: CommandId) -> io::Result<ControlOperation> {
        let record = self.authority.load(parent)?;
        if record.phase == ControlPhase::Aborted {
            return Ok(record);
        }
        self.authority.advance(&record, ControlUpdate::Abort)
    }

    /// 推进一个持久阶段或一条分片回执；离线分片不会阻止其余可用目标先行确认。
    pub fn step(&self, parent: CommandId) -> io::Result<ControlOperation> {
        self.step_until(parent, None)
    }

    fn step_until(
        &self,
        parent: CommandId,
        deadline: Option<Instant>,
    ) -> io::Result<ControlOperation> {
        let record = self.authority.load(parent)?;
        if record
            .targets
            .keys()
            .copied()
            .ne(self.members.iter().copied())
        {
            return Err(invalid("恢复拓扑与已登记控制操作不一致"));
        }
        if matches!(record.phase, ControlPhase::Done | ControlPhase::Aborted) {
            return Ok(record);
        }
        if record.phase == ControlPhase::Fanout
            && self
                .endpoints
                .read()
                .map_err(|_| invalid("控制分片句柄锁损坏"))?
                .is_empty()
        {
            self.telemetry.stalled();
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "目标分片均未连接",
            ));
        }
        if record.phase != ControlPhase::AwaitingAcks || record.acks.len() == record.targets.len() {
            return self.authority.advance(&record, ControlUpdate::Next);
        }
        // 只在复制句柄时持锁，分片回执或重新绑定都不占用此锁。
        let targets: Vec<_> = {
            let endpoints = self
                .endpoints
                .read()
                .map_err(|_| invalid("控制分片句柄锁损坏"))?;
            record
                .targets
                .iter()
                .filter(|(shard, _)| !record.acks.contains_key(shard))
                .filter_map(|(&shard, &id)| {
                    endpoints
                        .get(&shard)
                        .map(|handle| (shard, id, handle.clone()))
                })
                .collect()
        };
        let mut last = io::Error::new(io::ErrorKind::NotConnected, "目标分片尚未连接");
        let mut pending = targets.into_iter();
        while let Some((shard, id, handle)) = pending.next() {
            let timeout = match deadline {
                Some(end) => target_wait(time_left(end)?, pending.len(), self.timeout)?,
                None => self.timeout,
            };
            match self.dispatch(&handle, &record, id, timeout) {
                Ok(ack) => {
                    return self
                        .authority
                        .advance(&record, ControlUpdate::Ack(shard, ack));
                }
                Err(error) => last = error,
            }
        }
        self.telemetry.stalled();
        Err(last)
    }

    /// 驱动一个操作到终态。暂时不可达时指数退避，期限到达仍保留栅栏与全部回执。
    pub fn resume(&self, parent: CommandId, deadline: Instant) -> io::Result<ControlOperation> {
        self.drive(vec![parent], deadline)
            .remove(&parent)
            .expect("已登记唯一父命令")
    }

    /// 扫描一次持久索引，轮流推进此次读到的操作。逐项返回终态、超时或其他错误。
    pub fn recover_pending(
        &self,
        deadline: Instant,
    ) -> io::Result<BTreeMap<CommandId, io::Result<ControlOperation>>> {
        Ok(self.drive(self.authority.pending()?, deadline))
    }

    fn drive(
        &self,
        mut pending: Vec<CommandId>,
        deadline: Instant,
    ) -> BTreeMap<CommandId, io::Result<ControlOperation>> {
        let mut finished = BTreeMap::new();
        let mut delay = Duration::from_millis(5);
        while !pending.is_empty() {
            let mut progress = false;
            pending.retain(|parent| {
                let result =
                    time_left(deadline).and_then(|_| self.step_until(*parent, Some(deadline)));
                match result {
                    Ok(r) if matches!(r.phase, ControlPhase::Done | ControlPhase::Aborted) => {
                        finished.insert(*parent, Ok(r));
                        progress = true;
                        false
                    }
                    Ok(_) => {
                        progress = true;
                        true
                    }
                    Err(e)
                        if Instant::now() < deadline
                            && matches!(
                                e.kind(),
                                io::ErrorKind::TimedOut
                                    | io::ErrorKind::WouldBlock
                                    | io::ErrorKind::NotConnected
                                    | io::ErrorKind::BrokenPipe
                            ) =>
                    {
                        true
                    }
                    Err(e) => {
                        finished.insert(*parent, Err(e));
                        false
                    }
                }
            });
            if progress {
                delay = Duration::from_millis(5);
            } else if !pending.is_empty() {
                std::thread::sleep(delay.min(deadline.saturating_duration_since(Instant::now())));
                delay = delay.saturating_mul(2).min(Duration::from_secs(1));
            }
        }
        finished
    }

    fn dispatch(
        &self,
        handle: &ShardHandle,
        record: &ControlOperation,
        id: CommandId,
        timeout: Duration,
    ) -> io::Result<ControlAck> {
        let command = record.envelope(id, handle.control_primary_epoch());
        let rx = handle
            .submit_control_command(&self.authority.namespace(), command)
            .map_err(|error| {
                io::Error::new(
                    match error {
                        crate::ShardError::Busy => io::ErrorKind::WouldBlock,
                        _ => io::ErrorKind::NotConnected,
                    },
                    format!("控制命令入队失败 {error:?}"),
                )
            })?;
        let result = rx
            .recv_timeout(timeout)
            .map_err(|e| {
                io::Error::new(
                    match e {
                        std::sync::mpsc::RecvTimeoutError::Disconnected => {
                            io::ErrorKind::BrokenPipe
                        }
                        _ => io::ErrorKind::TimedOut,
                    },
                    "控制命令缺少持久回执",
                )
            })?
            .map_err(|error| {
                io::Error::new(
                    match error {
                        FormalError::Rejected(
                            RejectReason::EngineBusy | RejectReason::MigrationInProgress,
                        ) => io::ErrorKind::WouldBlock,
                        FormalError::Rejected(RejectReason::StalePrimaryEpoch) => {
                            io::ErrorKind::NotConnected
                        }
                        _ => io::ErrorKind::InvalidData,
                    },
                    format!("分片拒绝控制命令 {error:?}"),
                )
            })?;
        if let FormalOutcome::MassCancel(cancel) = result.outcome
            && result.command_id == id
            && result.seq != 0
            && result.summary.durable_seq == Some(result.seq)
        {
            Ok(ControlAck {
                shard_seq: result.seq,
                canceled: cancel.canceled_orders,
            })
        } else {
            Err(invalid("分片控制回执类型或身份不一致"))
        }
    }
}

/// 控制操作因没有可确认分片而无法推进的尝试次数。
#[derive(Default)]
pub struct ControlTelemetry {
    /// 准备派发时没有已登记句柄，或本轮尝试所有可联系目标后仍未取得分片回执的次数。
    stalls: AtomicU64,
    /// 次数已超过可表示范围。
    overflow: AtomicBool,
}

impl ControlTelemetry {
    fn stalled(&self) {
        if self
            .stalls
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .is_err()
        {
            self.overflow.store(true, Ordering::Relaxed);
        }
    }

    /// 返回停滞轮数和溢出标志，供外围告警使用。
    pub fn snapshot(&self) -> (u64, bool) {
        (
            self.stalls.load(Ordering::Relaxed),
            self.overflow.load(Ordering::Relaxed),
        )
    }
}

impl ControlOperation {
    fn envelope(&self, id: CommandId, primary: u64) -> CommandEnvelope {
        let (user, action) = match self.kind {
            ControlKind::UserMassCancel(user) => (Some(user), UserFenceAction::FenceAndCancelAll),
            ControlKind::GlobalMassCancel => (None, UserFenceAction::FenceAndCancelAll),
            ControlKind::UserKill {
                user,
                active,
                cancel_all,
            } => (Some(user), UserFenceAction::Kill { active, cancel_all }),
            ControlKind::GlobalKill { active, cancel_all } => {
                (None, UserFenceAction::Kill { active, cancel_all })
            }
        };
        let payload = match user {
            Some(user_id) => Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
                parent_command_id: self.parent,
                user_id,
                new_epoch: self.epoch,
                action,
                expected_primary_epoch: primary,
            }),
            None => Command::ApplyGlobalControlFenceShard(ApplyGlobalControlFenceShard {
                parent_command_id: self.parent,
                new_epoch: self.epoch,
                action,
                expected_primary_epoch: primary,
            }),
        };
        // 将固定子身份逐字节拆为会话二元组，换主和重试都使用相同别名。
        let bytes = id.get().to_be_bytes();
        let (high, low) = bytes.split_at(8);
        CommandEnvelope {
            command_id: id,
            session_id: u64::from_be_bytes(high.try_into().expect("高八字节")),
            client_seq: u64::from_be_bytes(low.try_into().expect("低八字节")),
            market_id: None,
            user_id: user,
            route_epoch: None,
            client_timestamp: None,
            received_at_unix_ns: None,
            payload,
        }
    }
}

/// 限制恢复轮询与等待回复的总时长；控制存储请求另受其客户端超时限制。
fn time_left(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "控制操作等待超时"))
}

/// 给本轮尚可联系的每个目标留出回复时间，避免早序分片耗尽整个恢复期限。
fn target_wait(left: Duration, others: usize, max: Duration) -> io::Result<Duration> {
    let targets = u32::try_from(others)
        .ok()
        .and_then(|n| n.checked_add(1))
        .ok_or_else(|| invalid("待确认分片数越界"))?;
    Ok((left / targets).min(max))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unresponsive_shard_leaves_time_for_later_targets() {
        let total = Duration::from_secs(1);
        let first = target_wait(total, 1, total).unwrap();
        let second = target_wait(total - first, 0, total).unwrap();
        assert_eq!(
            (first, second),
            (Duration::from_millis(500), Duration::from_millis(500))
        );
        assert_eq!(
            target_wait(total, 0, Duration::from_millis(10)).unwrap(),
            Duration::from_millis(10)
        );
        assert!(target_wait(total, usize::MAX, total).is_err());
    }
}
