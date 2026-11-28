//! 在一个分片序号内原子应用控制纪元、开关和撤单，并保存可重试的原始回执。
use super::*;
use crate::{cross_control::ControlKind, user_control::FenceReceipt};
use matching_domain::{KillScope, UserFenceAction, UserId};

/// 提取作用域、父命令、控制纪元、动作和期望主节点纪元。历史单机下限由独立入口处理。
fn fields(payload: &Command) -> Option<(Option<UserId>, CommandId, u64, UserFenceAction, u64)> {
    match payload {
        Command::ApplyUserControlFenceShard(f) if f.action != UserFenceAction::FenceOnly => Some((
            Some(f.user_id),
            f.parent_command_id,
            f.new_epoch,
            f.action,
            f.expected_primary_epoch,
        )),
        Command::ApplyGlobalControlFenceShard(f) => Some((
            None,
            f.parent_command_id,
            f.new_epoch,
            f.action,
            f.expected_primary_epoch,
        )),
        _ => None,
    }
}

/// 识别需要持久作用域回执的分片控制。
pub(super) fn coordinated(payload: &Command) -> bool {
    fields(payload).is_some()
}

/// 实时路径先核对预期纪元，再向实际持有写权的本地租约或认证复制写者校验。
pub(super) fn verify_owner(
    payload: &Command,
    gate: &mut LocalDurableCommit,
    place: Option<&PlaceRuntimeState>,
    identity: (ClusterId, ShardId),
) -> Result<(), FormalError> {
    let Some((_, _, _, _, expected)) = fields(payload) else {
        return Ok(());
    };
    let stale = FormalError::Rejected(RejectReason::StalePrimaryEpoch);
    if expected != gate.epoch {
        return Err(stale);
    }
    if gate.event_hasher.is_some() {
        gate.batcher
            .verify_control_owner(expected)
            .map_err(|_| stale)
    } else {
        place
            .and_then(|p| p.config.primary_lease.as_ref())
            .ok_or(stale)?
            .verify(identity.0, identity.1, expected)
            .map_err(|_| stale)
    }
}

impl FenceReceipt {
    /// 从首次执行的持久回执重建摘要，重试时传入空事件批次。
    fn execution(
        self,
        id: CommandId,
        events: Vec<EventEnvelope>,
        duplicate: bool,
    ) -> FormalExecution {
        FormalExecution {
            command_id: id,
            seq: self.seq,
            outcome: FormalOutcome::MassCancel(ShardMassCancelResult {
                canceled_orders: self.canceled,
            }),
            summary: CommandResultSummary {
                status: CommandStatus::Succeeded,
                first_event_index: Some(0),
                last_event_index: Some(self.last_event),
                durable_seq: Some(self.seq),
                reject_reason: None,
            },
            event_range: Some((0, self.last_event)),
            events,
            deduplicated: duplicate,
            market_data_child_opens: Vec::new(),
            market_data_open_details: Vec::new(),
            market_data_source: BTreeMap::new(),
        }
    }
}

/// 实时执行和重放共用确定性候选。在分配分片序号前排除旧纪元、冲突意图和无法完整提交的撤单。
pub(super) fn execute(
    context: ShardKillContext<'_>,
    epochs: &mut UserControlEpochs,
    command: CommandEnvelope,
) -> Result<FormalExecution, FormalError> {
    let invalid = FormalError::Rejected(RejectReason::InvalidOrderCombination);
    let (user, parent, epoch, action, expected) = fields(&command.payload).ok_or(invalid)?;
    if expected != context.epoch {
        return Err(FormalError::Rejected(RejectReason::StalePrimaryEpoch));
    }
    let kind = match (user, action) {
        (Some(user), UserFenceAction::FenceAndCancelAll) => ControlKind::UserMassCancel(user),
        (None, UserFenceAction::FenceAndCancelAll) => ControlKind::GlobalMassCancel,
        (Some(user), UserFenceAction::Kill { active, cancel_all }) => ControlKind::UserKill {
            user,
            active,
            cancel_all,
        },
        (None, UserFenceAction::Kill { active, cancel_all }) => {
            ControlKind::GlobalKill { active, cancel_all }
        }
        (_, UserFenceAction::FenceOnly) => return Err(invalid),
    };
    if epoch == 0
        || command.user_id != user
        || command.market_id.is_some()
        || command.route_epoch.is_some()
        || action.wire_code().is_none()
        || kind.derive(parent, context.identity.shard) != command.command_id
    {
        return Err(invalid);
    }
    let prior = epochs.receipt(user);
    // 旧命令即使还在普通短窗口内也不能重新切换开关或产生撤单。
    if let Some(r) = prior {
        if epoch < r.epoch {
            return Err(FormalError::ProtocolError);
        }
        if epoch == r.epoch {
            return if parent == r.parent && action == r.action {
                Ok(r.execution(command.command_id, Vec::new(), true))
            } else {
                Err(FormalError::ProtocolError)
            };
        }
    }
    if context
        .dedup
        .lookup(&command)
        .map_err(|_| FormalError::ProtocolError)?
        .is_some()
    {
        return Err(FormalError::ProtocolError);
    }
    let mut next_epochs = epochs
        .try_clone_preallocated()
        .map_err(|_| FormalError::Rejected(RejectReason::EngineBusy))?;
    if let Some(id) = user {
        if epoch <= epochs.floor(id) {
            return Err(FormalError::Rejected(RejectReason::StaleUserControlEpoch));
        }
        next_epochs
            .advance(id, epoch)
            .map_err(FormalError::Rejected)?;
    }
    let state = context.place.as_mut().ok_or(invalid)?;
    let scope = user.map_or(KillScope::Global, KillScope::User);
    let advanced = match user {
        Some(user_id) => EngineEvent::UserControlEpochAdvanced { user_id, epoch },
        None => EngineEvent::GlobalControlEpochAdvanced { epoch },
    };
    let mut payloads = Vec::new();
    let mut killed = prior.is_some_and(|r| r.killed);
    let cancel = match action {
        UserFenceAction::Kill { active, cancel_all } => {
            killed = active;
            payloads.push((
                None,
                if active {
                    EngineEvent::KillSwitchActivated { scope }
                } else {
                    EngineEvent::KillSwitchCleared { scope }
                },
            ));
            cancel_all
        }
        UserFenceAction::FenceAndCancelAll => true,
        UserFenceAction::FenceOnly => return Err(invalid),
    };
    let mut canceled = 0_u64;
    // 使用独立的候选副本；一张订单释放失败时保留整个分片的旧状态。
    let mut next = None;
    if cancel {
        let mut books = context.markets.clone();
        let mut registry = context.registry.as_ref().ok_or(invalid)?.clone();
        let mut ids = state
            .ids
            .try_clone_preallocated()
            .map_err(|_| FormalError::Rejected(RejectReason::EngineBusy))?;
        let scope = user.map_or(ShardCancelScope::All, |user_id| ShardCancelScope::User {
            user_id,
        });
        for (market, book) in &mut books {
            let before = book.clone();
            let plan = book
                .plan_mass_cancel(scope, &registry)
                .map_err(FormalError::Rejected)?;
            let (count, mut events) = book
                .apply_mass_cancel(&mut registry, plan)
                .expect("独立的分片控制撤单候选不能漂移");
            canceled = canceled
                .checked_add(count.canceled_orders)
                .ok_or(FormalError::Rejected(RejectReason::ArithmeticOverflow))?;
            for event in &mut events {
                if let EngineEvent::OrderCanceled {
                    order_id, reason, ..
                } = event
                {
                    if matches!(action, UserFenceAction::Kill { .. }) {
                        *reason = matching_domain::CancelReason::KillSwitch;
                    }
                    ids.terminal_in_market(*market, *order_id);
                }
            }
            payloads.extend(
                crate::place_candidate::release_terminal_reservations(
                    &before,
                    book,
                    &mut registry,
                    events,
                )
                .map_err(FormalError::Rejected)?
                .into_iter()
                .map(|event| (Some(*market), event)),
            );
        }
        next = Some((books, registry, ids));
    }
    // 纪元事件标识该作用域在分片上的完整切面，排在所有订单与资金事件之后。
    payloads.push((None, advanced));
    if !u64::try_from(payloads.len())
        .is_ok_and(|n| event_count_fits(n, context.identity.event_capacity))
    {
        return Err(FormalError::Rejected(RejectReason::ArithmeticOverflow));
    }
    let prepared = context.dedup.prepare(&command);
    let seq = context.seq.allocate().expect("分片序号耗尽，停止处理");
    let events: Vec<_> = payloads
        .into_iter()
        .enumerate()
        .map(|(i, (market_id, payload))| EventEnvelope {
            cluster_id: context.identity.cluster,
            shard_id: context.identity.shard,
            shard_seq: seq,
            event_index: u32::try_from(i).expect("事件索引已经预检"),
            command_id: command.command_id,
            engine_time: context.clock.now().get(),
            market_id,
            payload,
        })
        .collect();
    let receipt = FenceReceipt {
        epoch,
        parent,
        action,
        killed,
        seq,
        canceled,
        last_event: events.last().expect("栅栏总有纪元事件").event_index,
    };
    next_epochs.record_receipt(user, receipt);
    if let Some((books, registry, ids)) = next {
        *context.markets = books;
        *context.registry = Some(registry);
        state.ids = ids;
    }
    *epochs = next_epochs;
    let result = receipt.execution(command.command_id, events, false);
    context.dedup.insert_prepared(&command, prepared, &result);
    Ok(result)
}

#[cfg(test)]
mod tests;
