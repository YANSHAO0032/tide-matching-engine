//! 分片内有界命令结果与会话别名。
use crate::{FormalExecution, FormalOutcome};
use matching_domain::{
    Command, CommandEnvelope, CommandId, CommandResultSummary, MarketId, UserId,
};
use std::collections::VecDeque;

/// 分片内幂等窗口与会话缓存的容量。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdempotencyConfig {
    /// 首次执行结果的最大保留数量。
    pub command_capacity: usize,
    /// 保留会话的最大数量。
    pub session_capacity: usize,
    /// 单个会话最多保留的序号别名数量。
    pub aliases_per_session: usize,
}
impl Default for IdempotencyConfig {
    fn default() -> Self {
        Self {
            command_capacity: 4096,
            session_capacity: 1024,
            aliases_per_session: 64,
        }
    }
}
impl IdempotencyConfig {
    /// 所有容量必须严格为正。
    pub(crate) fn valid(self) -> bool {
        self.command_capacity > 0 && self.session_capacity > 0 && self.aliases_per_session > 0
    }
}

/// 不受路由与传输元数据影响的业务意图。
#[derive(Clone, Debug, PartialEq, Eq)]
struct Intent {
    /// 业务操作关联的市场。
    market: Option<MarketId>,
    /// 业务操作关联的用户。
    user: Option<UserId>,
    /// 冻结比较的业务命令载荷。
    payload: Command,
}

/// 在业务提交之前冻结的命令身份和完整意图。
pub(crate) struct PreparedIntent {
    /// 首次执行业务命令的身份。
    command_id: CommandId,
    /// 已分配并复制的业务意图。
    intent: Intent,
}
impl From<&CommandEnvelope> for Intent {
    fn from(c: &CommandEnvelope) -> Self {
        Self {
            market: c.market_id,
            user: c.user_id,
            payload: c.payload.clone(),
        }
    }
}
/// 首次执行后保存的有界结果，不保存事件批次。
#[derive(Clone, Debug)]
struct Entry {
    /// 命令身份。
    command_id: CommandId,
    /// 原始业务意图。
    intent: Intent,
    /// 首次执行序号。
    seq: u64,
    /// 首次业务结果。
    outcome: FormalOutcome,
    /// 原始命令执行摘要。
    summary: CommandResultSummary,
    /// 首次事件的索引范围。
    event_range: Option<(u32, u32)>,
}
/// 单会话保存的序号别名，槽位在启动时预分配。
struct Session {
    /// 已占用槽位的会话身份。
    id: Option<u64>,
    /// 曾观察到的最高序号。
    highest: u64,
    /// 当前保留的序号和命令身份。
    aliases: VecDeque<(u64, CommandId)>,
}
/// 仅由分片线程修改的确定性先进先出窗口。
pub(crate) struct DedupWindow {
    /// 固定容量。
    config: IdempotencyConfig,
    /// 按首次执行顺序保存的命令。
    entries: VecDeque<Entry>,
    /// 预分配的会话槽位。
    sessions: Vec<Session>,
    /// 会话槽位首次使用顺序。
    session_order: VecDeque<usize>,
}
impl DedupWindow {
    /// 在启动阶段预分配窗口与所有会话槽位。
    pub(crate) fn new(config: IdempotencyConfig) -> Result<Self, ()> {
        if !config.valid() {
            return Err(());
        }
        let mut entries = VecDeque::new();
        entries
            .try_reserve_exact(config.command_capacity)
            .map_err(|_| ())?;
        let mut session_order = VecDeque::new();
        session_order
            .try_reserve_exact(config.session_capacity)
            .map_err(|_| ())?;
        let mut sessions = Vec::new();
        sessions
            .try_reserve_exact(config.session_capacity)
            .map_err(|_| ())?;
        for _ in 0..config.session_capacity {
            let mut aliases = VecDeque::new();
            aliases
                .try_reserve_exact(config.aliases_per_session)
                .map_err(|_| ())?;
            sessions.push(Session {
                id: None,
                highest: 0,
                aliases,
            });
        }
        Ok(Self {
            config,
            entries,
            sessions,
            session_order,
        })
    }
    /// 按固定容量复制候选窗口；预留容器失败时不改变原窗口。
    pub(crate) fn try_clone_preallocated(&self) -> Result<Self, ()> {
        let mut copy = Self::new(self.config)?;
        copy.entries.extend(self.entries.iter().cloned());
        copy.session_order
            .extend(self.session_order.iter().copied());
        for (target, source) in copy.sessions.iter_mut().zip(&self.sessions) {
            target.id = source.id;
            target.highest = source.highest;
            target.aliases.extend(source.aliases.iter().copied());
        }
        Ok(copy)
    }
    /// 别名冲突及意图冲突不占序号，不修改任何缓存。
    pub(crate) fn lookup(&self, c: &CommandEnvelope) -> Result<Option<FormalExecution>, ()> {
        if let Some(session) = self.sessions.iter().find(|s| s.id == Some(c.session_id))
            && session
                .aliases
                .iter()
                .any(|(seq, id)| *seq == c.client_seq && *id != c.command_id)
        {
            return Err(());
        }
        if let Some(entry) = self.entries.iter().find(|e| e.command_id == c.command_id) {
            if entry.intent.market != c.market_id
                || entry.intent.user != c.user_id
                || entry.intent.payload != c.payload
            {
                return Err(());
            }
            return Ok(Some(FormalExecution {
                command_id: c.command_id,
                seq: entry.seq,
                outcome: entry.outcome.clone(),
                summary: entry.summary.clone(),
                events: Vec::new(),
                event_range: entry.event_range,
                deduplicated: true,
            }));
        }
        Ok(None)
    }
    /// 在业务执行之前复制身份载荷，避免提交后才分配其动态字段。
    pub(crate) fn prepare(&self, c: &CommandEnvelope) -> PreparedIntent {
        PreparedIntent {
            command_id: c.command_id,
            intent: Intent::from(c),
        }
    }
    /// 只在首次业务执行完成后登记，按首次执行顺序淘汰。
    pub(crate) fn insert_prepared(
        &mut self,
        c: &CommandEnvelope,
        prepared: PreparedIntent,
        result: &FormalExecution,
    ) {
        assert_eq!(prepared.command_id, c.command_id);
        assert!(
            !self
                .entries
                .iter()
                .any(|entry| entry.command_id == c.command_id)
        );
        if self.entries.len() == self.config.command_capacity {
            let old = self
                .entries
                .pop_front()
                .expect("去重先进先出状态缺失")
                .command_id;
            for session in &mut self.sessions {
                session.aliases.retain(|(_, id)| *id != old);
            }
        }
        self.entries.push_back(Entry {
            command_id: c.command_id,
            intent: prepared.intent,
            seq: result.seq,
            outcome: result.outcome.clone(),
            summary: result.summary.clone(),
            event_range: result.event_range,
        });
        self.remember_alias(c);
    }
    /// 测试中模拟连续的预备与登记。
    #[cfg(test)]
    fn insert(&mut self, c: &CommandEnvelope, result: &FormalExecution) {
        let prepared = self.prepare(c);
        self.insert_prepared(c, prepared, result);
    }
    /// 维护有限会话别名；不刷新命令的保留顺序。
    pub(crate) fn remember_alias(&mut self, c: &CommandEnvelope) {
        let index = match self
            .sessions
            .iter()
            .position(|s| s.id == Some(c.session_id))
        {
            Some(index) => index,
            None => {
                let index = match self.sessions.iter().position(|s| s.id.is_none()) {
                    Some(index) => index,
                    None => self
                        .session_order
                        .pop_front()
                        .expect("会话先进先出状态缺失"),
                };
                let session = &mut self.sessions[index];
                session.id = Some(c.session_id);
                session.highest = 0;
                session.aliases.clear();
                self.session_order.push_back(index);
                index
            }
        };
        let session = &mut self.sessions[index];
        session.highest = session.highest.max(c.client_seq);
        if !session
            .aliases
            .iter()
            .any(|(seq, id)| *seq == c.client_seq && *id == c.command_id)
        {
            if session.aliases.len() == self.config.aliases_per_session {
                session.aliases.pop_front();
            }
            session.aliases.push_back((c.client_seq, c.command_id));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use matching_domain::{MassCancelShard, ShardCancelScope, ShardMassCancelResult};
    /// 构造带指定身份与市场的测试命令。
    fn cmd(id: u128, session: u64, seq: u64, market: u32) -> CommandEnvelope {
        CommandEnvelope {
            command_id: CommandId(id),
            session_id: session,
            client_seq: seq,
            market_id: Some(MarketId(market)),
            user_id: None,
            route_epoch: Some(1),
            client_timestamp: None,
            received_at_unix_ns: None,
            payload: Command::MassCancelShard(MassCancelShard {
                scope: ShardCancelScope::Market {
                    market_id: MarketId(market),
                },
            }),
        }
    }
    /// 构造指定身份和序号的测试结果。
    fn result(id: u128, seq: u64) -> FormalExecution {
        FormalExecution {
            command_id: CommandId(id),
            seq,
            outcome: FormalOutcome::MassCancel(ShardMassCancelResult { canceled_orders: 1 }),
            summary: matching_domain::CommandResultSummary {
                status: matching_domain::CommandStatus::Succeeded,
                first_event_index: Some(0),
                last_event_index: Some(0),
                durable_seq: Some(seq),
                reject_reason: None,
            },
            events: Vec::new(),
            event_range: Some((0, 0)),
            deduplicated: false,
        }
    }
    #[test]
    fn retries_do_not_refresh_fifo_and_eviction_removes_aliases() {
        let mut d = DedupWindow::new(IdempotencyConfig {
            command_capacity: 2,
            session_capacity: 2,
            aliases_per_session: 2,
        })
        .unwrap();
        let a = cmd(1, 1, 1, 7);
        let b = cmd(2, 1, 2, 7);
        d.insert(&a, &result(1, 1));
        d.insert(&b, &result(2, 2));
        let mut retry = a.clone();
        retry.session_id = 2;
        retry.client_seq = 7;
        retry.route_epoch = Some(9);
        retry.client_timestamp = Some(99);
        let cached = d.lookup(&retry).unwrap().unwrap();
        assert_eq!(cached.seq, 1);
        assert_eq!(cached.event_range, Some((0, 0)));
        assert!(cached.events.is_empty());
        assert!(cached.deduplicated);
        d.remember_alias(&retry);
        let c = cmd(3, 1, 3, 7);
        d.insert(&c, &result(3, 3));
        assert!(d.lookup(&a).unwrap().is_none());
        assert!(d.lookup(&b).unwrap().is_some());
        let mut collision = retry.clone();
        collision.command_id = CommandId(4);
        assert!(d.lookup(&collision).unwrap().is_none());
    }
    #[test]
    fn session_and_command_identity_collisions_fail_without_mutation() {
        let mut d = DedupWindow::new(IdempotencyConfig::default()).unwrap();
        let a = cmd(1, 1, 3, 7);
        d.insert(&a, &result(1, 1));
        let mut b = a.clone();
        b.command_id = CommandId(2);
        assert!(d.lookup(&b).is_err());
        let mut c = a.clone();
        c.market_id = Some(MarketId(8));
        assert!(d.lookup(&c).is_err());
        assert!(d.lookup(&a).unwrap().is_some());
    }

    #[test]
    fn session_capacity_and_alias_eviction_remain_bounded() {
        let mut d = DedupWindow::new(IdempotencyConfig {
            command_capacity: 4,
            session_capacity: 1,
            aliases_per_session: 1,
        })
        .unwrap();
        let a = cmd(1, 1, 9, 7);
        let b = cmd(2, 1, 10, 7);
        d.insert(&a, &result(1, 1));
        d.insert(&b, &result(2, 2));
        assert!(d.lookup(&a).unwrap().is_some());
        let old_seq_new_id = cmd(3, 1, 9, 7);
        assert!(d.lookup(&old_seq_new_id).unwrap().is_none());
        let reconnect = cmd(1, 2, 2, 7);
        assert!(d.lookup(&reconnect).unwrap().is_some());
        d.remember_alias(&reconnect);
        assert_eq!(d.session_order.len(), 1);
        assert_eq!(d.sessions[0].id, Some(2));
        assert_eq!(d.sessions[0].highest, 2);
        assert!(d.lookup(&b).unwrap().is_some());
    }

    #[test]
    fn prepared_identity_is_frozen_before_business_execution() {
        let mut d = DedupWindow::new(IdempotencyConfig::default()).unwrap();
        let original = cmd(1, 1, 1, 7);
        let prepared = d.prepare(&original);
        let mut changed = original.clone();
        changed.market_id = Some(MarketId(8));
        d.insert_prepared(&original, prepared, &result(1, 1));
        assert!(d.lookup(&original).unwrap().is_some());
        assert!(d.lookup(&changed).is_err());
    }
    #[test]
    fn candidate_clone_keeps_all_limits_and_does_not_change_original() {
        let config = IdempotencyConfig {
            command_capacity: 3,
            session_capacity: 2,
            aliases_per_session: 4,
        };
        let mut original = DedupWindow::new(config).unwrap();
        let first = cmd(1, 1, 1, 7);
        original.insert(&first, &result(1, 1));
        let mut candidate = original.try_clone_preallocated().unwrap();
        assert!(candidate.entries.capacity() >= config.command_capacity);
        assert!(candidate.session_order.capacity() >= config.session_capacity);
        assert!(
            candidate
                .sessions
                .iter()
                .all(|s| s.aliases.capacity() >= config.aliases_per_session)
        );
        let second = cmd(2, 1, 2, 7);
        candidate.insert(&second, &result(2, 2));
        assert!(candidate.lookup(&second).unwrap().is_some());
        assert!(original.lookup(&second).unwrap().is_none());
    }
}
