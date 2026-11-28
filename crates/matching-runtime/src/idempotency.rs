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
        let mut payload = c.payload.clone();
        if let Command::ShardKillSwitch(ref mut control) = payload {
            // 主节点纪元是重试的 fencing 元数据，不属于稳定业务意图。
            control.expected_primary_epoch = 0;
        }
        Self {
            market: c.market_id,
            user: c.user_id,
            payload,
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
    /// 跨分片迁入时记录首次执行所在分片，以保留原始去重回复序号。
    original_shard: Option<matching_domain::ShardId>,
    /// 首次执行序号。
    seq: u64,
    /// 首次业务结果。
    outcome: FormalOutcome,
    /// 原始命令执行摘要。
    summary: CommandResultSummary,
    /// 首次事件的索引范围。
    event_range: Option<(u32, u32)>,
}
/// 核对首次命令的结果类型、订单身份，以及控制动作允许的撤单数和事件范围。
fn outcome_matches(command: &Command, outcome: &FormalOutcome, range: Option<(u32, u32)>) -> bool {
    let action = match command {
        Command::ApplyUserControlFenceShard(f)
            if f.action == matching_domain::UserFenceAction::FenceOnly =>
        {
            return matches!(outcome, FormalOutcome::UserFence(epoch) if *epoch >= f.new_epoch)
                && range == Some((0, 0));
        }
        Command::ApplyUserControlFenceShard(f) => Some(f.action),
        Command::ApplyGlobalControlFenceShard(f) => Some(f.action),
        _ => None,
    };
    if let Some(action) = action {
        return matches!((outcome, range),
            (FormalOutcome::MassCancel(result), Some((0, last)))
            if crate::user_control::control_events_match(action, result.canceled_orders, last));
    }
    match (command, outcome) {
        (_, FormalOutcome::Rejected(_)) => true,
        (Command::PlaceLimit(place), FormalOutcome::Placed(id)) => place.order.order_id == *id,
        (Command::Cancel(input), FormalOutcome::Canceled(id)) => input.order_id == *id,
        (Command::MassCancelShard(_), FormalOutcome::MassCancel(_)) => true,
        (Command::ShardKillSwitch(_), FormalOutcome::ShardControl) => true,
        (Command::UpdateFeeSchedule(input), FormalOutcome::FeeScheduleUpdated(version)) => {
            input.schedule.version() == *version
        }
        (
            Command::CreateMarket(_)
            | Command::TriggerCircuitBreaker(_)
            | Command::MarketKillSwitch(_)
            | Command::UpdateReferencePrice(_)
            | Command::UpdateMarket(_)
            | Command::StartSession(_)
            | Command::OpenMarket(_)
            | Command::SuspendMarket(_)
            | Command::ResumeMarket(_)
            | Command::EndSession(_)
            | Command::CloseMarket(_),
            FormalOutcome::MarketManaged,
        ) => true,
        _ => false,
    }
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
    /// 按窗口保留顺序复制指定市场的命令条目，为尚无来源标记的条目补记原执行分片。
    fn market_entries(
        &self,
        market: MarketId,
        shard: matching_domain::ShardId,
    ) -> impl Iterator<Item = Entry> + '_ {
        self.entries
            .iter()
            .filter(move |e| e.intent.market == Some(market))
            .cloned()
            .map(move |mut e| {
                e.original_shard.get_or_insert(shard);
                e
            })
    }

    /// 单市场摘要使用原执行分片与序号，并排除同一会话在其他市场的活动。
    pub(crate) fn write_market_state(
        &self,
        market: MarketId,
        shard: matching_domain::ShardId,
        w: &mut matching_domain::snapshot_wire::SnapshotEncoder,
    ) -> Result<(), matching_domain::snapshot_wire::SnapshotWireError> {
        use matching_domain::snapshot_wire::SnapshotWireError;
        let entries: VecDeque<_> = self.market_entries(market, shard).collect();
        let ids: std::collections::BTreeSet<_> = entries.iter().map(|e| e.command_id).collect();
        let mut sessions = std::collections::BTreeMap::new();
        for session in self.sessions.iter().filter(|s| s.id.is_some()) {
            let aliases: VecDeque<_> = session
                .aliases
                .iter()
                .copied()
                .filter(|(_, id)| ids.contains(id))
                .collect();
            if !aliases.is_empty() {
                sessions.insert(session.id.ok_or(SnapshotWireError)?, aliases);
            }
        }
        let mut scoped = Self::new(IdempotencyConfig {
            command_capacity: entries.len().max(1),
            session_capacity: sessions.len().max(1),
            aliases_per_session: sessions.values().map(VecDeque::len).max().unwrap_or(1),
        })
        .map_err(|_| SnapshotWireError)?;
        scoped.entries = entries;
        for (index, (id, aliases)) in sessions.into_iter().enumerate() {
            scoped.sessions[index] = Session {
                id: Some(id),
                highest: aliases.iter().map(|a| a.0).max().ok_or(SnapshotWireError)?,
                aliases,
            };
            scoped.session_order.push_back(index);
        }
        scoped.write_snapshot(w)
    }

    /// 在隔离分片候选中迁移市场窗口；容量或身份冲突时调用方丢弃候选。
    pub(crate) fn transfer_market_to(
        &mut self,
        target: &mut Self,
        market: MarketId,
        shard: matching_domain::ShardId,
    ) -> Result<(), ()> {
        let moved: std::collections::BTreeSet<_> = self
            .market_entries(market, shard)
            .map(|e| e.command_id)
            .collect();
        if target.entries.len().checked_add(moved.len()).ok_or(())? > target.config.command_capacity
            || target.entries.iter().any(|e| moved.contains(&e.command_id))
        {
            return Err(());
        }
        for index in &self.session_order {
            let session = &self.sessions[*index];
            let mut incoming = session
                .aliases
                .iter()
                .copied()
                .filter(|(_, id)| moved.contains(id))
                .peekable();
            if incoming.peek().is_none() {
                continue;
            }
            let slot = if let Some(slot) = target.sessions.iter().position(|s| s.id == session.id) {
                slot
            } else {
                let slot = target
                    .sessions
                    .iter()
                    .position(|s| s.id.is_none())
                    .ok_or(())?;
                target.session_order.push_back(slot);
                slot
            };
            let dest = &mut target.sessions[slot];
            dest.id = session.id;
            dest.highest = dest.highest.max(session.highest);
            for alias in incoming {
                if dest.aliases.len() == target.config.aliases_per_session
                    || dest.aliases.iter().any(|(seq, _)| *seq == alias.0)
                {
                    return Err(());
                }
                dest.aliases.push_back(alias);
            }
        }
        target.entries.extend(self.market_entries(market, shard));
        self.entries.retain(|e| !moved.contains(&e.command_id));
        for session in &mut self.sessions {
            session.aliases.retain(|(_, id)| !moved.contains(id));
        }
        Ok(())
    }
    /// 按会话淘汰次序重排内部槽位，保留命令和别名先后次序。
    pub(crate) fn write_canonical_state(
        &self,
        writer: &mut matching_domain::snapshot_wire::SnapshotEncoder,
    ) -> Result<(), matching_domain::snapshot_wire::SnapshotWireError> {
        use matching_domain::snapshot_wire::SnapshotWireError;
        let mut canonical = Self::new(self.config).map_err(|_| SnapshotWireError)?;
        canonical.entries.extend(self.entries.iter().cloned());
        let mut seen = std::collections::BTreeSet::new();
        for (target, index) in self.session_order.iter().copied().enumerate() {
            let source = self.sessions.get(index).ok_or(SnapshotWireError)?;
            if source.id.is_none() || !seen.insert(index) {
                return Err(SnapshotWireError);
            }
            let slot = canonical
                .sessions
                .get_mut(target)
                .ok_or(SnapshotWireError)?;
            slot.id = source.id;
            slot.highest = source.highest;
            slot.aliases.extend(source.aliases.iter().copied());
            canonical.session_order.push_back(target);
        }
        if self
            .sessions
            .iter()
            .filter(|session| session.id.is_some())
            .count()
            != seen.len()
        {
            return Err(SnapshotWireError);
        }
        canonical.write_snapshot(writer)
    }
    /// 保存原始业务意图、执行摘要及会话槽位和淘汰次序。
    pub(crate) fn write_snapshot(
        &self,
        writer: &mut matching_domain::snapshot_wire::SnapshotEncoder,
    ) -> Result<(), matching_domain::snapshot_wire::SnapshotWireError> {
        use matching_domain::snapshot_wire::{
            SnapshotEncoder, SnapshotWireError, encode_reject_reason,
        };
        let mut w = SnapshotEncoder::new(256 * 1024 * 1024);
        // 合法容量严格为正；零前缀标记逐条携带原分片身份的新版窗口。
        let has_migrated = self.entries.iter().any(|e| e.original_shard.is_some());
        if has_migrated {
            w.u64(0)?;
        }
        for value in [
            self.config.command_capacity,
            self.config.session_capacity,
            self.config.aliases_per_session,
        ] {
            w.u64(u64::try_from(value).map_err(|_| SnapshotWireError)?)?;
        }
        w.u32(u32::try_from(self.entries.len()).map_err(|_| SnapshotWireError)?)?;
        for entry in &self.entries {
            if !outcome_matches(&entry.intent.payload, &entry.outcome, entry.event_range) {
                return Err(SnapshotWireError);
            }
            let envelope = CommandEnvelope {
                command_id: entry.command_id,
                session_id: 0,
                client_seq: 0,
                market_id: entry.intent.market,
                user_id: entry.intent.user,
                route_epoch: None,
                client_timestamp: None,
                received_at_unix_ns: None,
                payload: entry.intent.payload.clone(),
            };
            let command = matching_domain::command_wal_codec::encode_wal_command(&envelope)
                .map_err(|_| SnapshotWireError)?;
            w.bytes(&command)?;
            if has_migrated {
                match entry.original_shard {
                    Some(shard) => {
                        w.u8(1)?;
                        w.u16(shard.get())?;
                    }
                    None => w.u8(0)?,
                }
            }
            w.u64(entry.seq)?;
            match entry.outcome {
                FormalOutcome::FeeScheduleUpdated(version) => {
                    w.u8(5)?;
                    w.u64(version)?;
                }
                FormalOutcome::MarketManaged => w.u8(4)?,
                FormalOutcome::ShardControl => w.u8(6)?,
                FormalOutcome::Canceled(id) => {
                    w.u8(7)?;
                    w.u128(id.get())?;
                }
                FormalOutcome::Placed(id) => {
                    w.u8(0)?;
                    w.u128(id.get())?
                }
                FormalOutcome::UserFence(epoch) => {
                    w.u8(1)?;
                    w.u64(epoch)?
                }
                FormalOutcome::MassCancel(result) => {
                    w.u8(2)?;
                    w.u64(result.canceled_orders)?
                }
                FormalOutcome::Rejected(reason) => {
                    w.u8(3)?;
                    encode_reject_reason(&mut w, reason)?
                }
            }
            match entry.event_range {
                None => w.u8(0)?,
                Some((first, last)) => {
                    w.u8(1)?;
                    w.u32(first)?;
                    w.u32(last)?
                }
            }
            let (status, reason) = match entry.outcome {
                FormalOutcome::Rejected(reason) => {
                    (matching_domain::CommandStatus::Rejected, Some(reason))
                }
                _ => (matching_domain::CommandStatus::Succeeded, None),
            };
            if entry.summary.status != status
                || entry.summary.reject_reason != reason
                || entry.summary.durable_seq != Some(entry.seq)
                || entry.summary.first_event_index != entry.event_range.map(|r| r.0)
                || entry.summary.last_event_index != entry.event_range.map(|r| r.1)
            {
                return Err(SnapshotWireError);
            }
        }
        for session in &self.sessions {
            match session.id {
                None => w.u8(0)?,
                Some(id) => {
                    w.u8(1)?;
                    w.u64(id)?;
                    w.u64(session.highest)?;
                    w.u32(u32::try_from(session.aliases.len()).map_err(|_| SnapshotWireError)?)?;
                    for (seq, id) in &session.aliases {
                        w.u64(*seq)?;
                        w.u128(id.get())?;
                    }
                }
            }
        }
        w.u32(u32::try_from(self.session_order.len()).map_err(|_| SnapshotWireError)?)?;
        for index in &self.session_order {
            w.u64(u64::try_from(*index).map_err(|_| SnapshotWireError)?)?;
        }
        writer.bytes(&w.finish())
    }

    /// 使用可信容量和屏障序号恢复完整去重窗口。
    pub(crate) fn read_snapshot(
        reader: &mut matching_domain::snapshot_wire::SnapshotDecoder<'_>,
        config: IdempotencyConfig,
        snapshot_seq: u64,
    ) -> Result<Self, matching_domain::snapshot_wire::SnapshotWireError> {
        use matching_domain::{
            CommandStatus, ShardMassCancelResult,
            snapshot_wire::{SnapshotDecoder, SnapshotWireError, decode_reject_reason},
        };
        let bytes = reader.bytes()?;
        let mut r = SnapshotDecoder::new(bytes, 256 * 1024 * 1024)?;
        let first = r.u64()?;
        let has_migrated = first == 0;
        let capacity = if has_migrated { r.u64()? } else { first };
        if usize::try_from(capacity).ok() != Some(config.command_capacity) {
            return Err(SnapshotWireError);
        }
        for expected in [config.session_capacity, config.aliases_per_session] {
            if usize::try_from(r.u64()?).ok() != Some(expected) {
                return Err(SnapshotWireError);
            }
        }
        let count = usize::try_from(r.u32()?).map_err(|_| SnapshotWireError)?;
        if count > config.command_capacity || count > r.remaining() / 14 {
            return Err(SnapshotWireError);
        }
        let mut window = Self::new(config).map_err(|_| SnapshotWireError)?;
        let mut previous_seq = 0;
        for _ in 0..count {
            let command = matching_domain::command_wal_codec::decode_wal_command(r.bytes()?)
                .map_err(|_| SnapshotWireError)?;
            if command.session_id != 0
                || command.client_seq != 0
                || command.route_epoch.is_some()
                || command.client_timestamp.is_some()
                || command.received_at_unix_ns.is_some()
                || window
                    .entries
                    .iter()
                    .any(|entry| entry.command_id == command.command_id)
            {
                return Err(SnapshotWireError);
            }
            let original_shard = if has_migrated {
                match r.u8()? {
                    0 => None,
                    1 if command.market_id.is_some() => {
                        Some(matching_domain::ShardId::new(r.u16()?))
                    }
                    _ => return Err(SnapshotWireError),
                }
            } else {
                None
            };
            let seq = r.u64()?;
            if seq == 0 {
                return Err(SnapshotWireError);
            }
            if original_shard.is_none() {
                if seq <= previous_seq || seq > snapshot_seq {
                    return Err(SnapshotWireError);
                }
                previous_seq = seq;
            }
            let outcome = match r.u8()? {
                0 => FormalOutcome::Placed(matching_domain::OrderId::new(r.u128()?)),
                1 => FormalOutcome::UserFence(r.u64()?),
                2 => FormalOutcome::MassCancel(ShardMassCancelResult {
                    canceled_orders: r.u64()?,
                }),
                3 => FormalOutcome::Rejected(decode_reject_reason(&mut r)?),
                4 => FormalOutcome::MarketManaged,
                5 => FormalOutcome::FeeScheduleUpdated(r.u64()?),
                6 => FormalOutcome::ShardControl,
                7 => FormalOutcome::Canceled(matching_domain::OrderId(r.u128()?)),
                _ => return Err(SnapshotWireError),
            };
            let event_range = match r.u8()? {
                0 => None,
                1 => {
                    let first = r.u32()?;
                    let last = r.u32()?;
                    if first != 0 || last < first {
                        return Err(SnapshotWireError);
                    }
                    Some((first, last))
                }
                _ => return Err(SnapshotWireError),
            };
            if !outcome_matches(&command.payload, &outcome, event_range) {
                return Err(SnapshotWireError);
            }
            let (status, reject_reason) = match outcome {
                FormalOutcome::Rejected(reason) => (CommandStatus::Rejected, Some(reason)),
                _ => (CommandStatus::Succeeded, None),
            };
            let summary = CommandResultSummary {
                status,
                first_event_index: event_range.map(|r| r.0),
                last_event_index: event_range.map(|r| r.1),
                durable_seq: Some(seq),
                reject_reason,
            };
            window.entries.push_back(Entry {
                command_id: command.command_id,
                intent: Intent::from(&command),
                original_shard,
                seq,
                outcome,
                summary,
                event_range,
            });
        }
        let mut session_ids = std::collections::BTreeSet::new();
        for session in &mut window.sessions {
            match r.u8()? {
                0 => {}
                1 => {
                    let id = r.u64()?;
                    if !session_ids.insert(id) {
                        return Err(SnapshotWireError);
                    }
                    session.id = Some(id);
                    session.highest = r.u64()?;
                    let count = usize::try_from(r.u32()?).map_err(|_| SnapshotWireError)?;
                    if count > config.aliases_per_session || count > r.remaining() / 24 {
                        return Err(SnapshotWireError);
                    }
                    let mut seqs = std::collections::BTreeSet::new();
                    for _ in 0..count {
                        let seq = r.u64()?;
                        let id = CommandId::new(r.u128()?);
                        if seq > session.highest
                            || !seqs.insert(seq)
                            || !window.entries.iter().any(|entry| entry.command_id == id)
                        {
                            return Err(SnapshotWireError);
                        }
                        session.aliases.push_back((seq, id));
                    }
                }
                _ => return Err(SnapshotWireError),
            }
        }
        let count = usize::try_from(r.u32()?).map_err(|_| SnapshotWireError)?;
        if count != session_ids.len() || count > r.remaining() / 8 {
            return Err(SnapshotWireError);
        }
        let mut indexes = std::collections::BTreeSet::new();
        for _ in 0..count {
            let index = usize::try_from(r.u64()?).map_err(|_| SnapshotWireError)?;
            if index >= window.sessions.len()
                || window.sessions[index].id.is_none()
                || !indexes.insert(index)
            {
                return Err(SnapshotWireError);
            }
            window.session_order.push_back(index);
        }
        r.finish()?;
        Ok(window)
    }
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
            let payload_matches = match (&entry.intent.payload, &c.payload) {
                (Command::ShardKillSwitch(left), Command::ShardKillSwitch(right)) => {
                    left.target_shard_id == right.target_shard_id
                        && left.parent_command_id == right.parent_command_id
                        && left.active == right.active
                        && left.cancel_all == right.cancel_all
                }
                (left, right) => left == right,
            };
            if entry.intent.market != c.market_id
                || entry.intent.user != c.user_id
                || !payload_matches
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
                market_data_child_opens: Vec::new(),
                market_data_open_details: Vec::new(),
                market_data_source: Default::default(),
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
            original_shard: None,
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
    #[test]
    fn shard_control_intent_ignores_refreshed_epoch_but_not_policy() {
        let mut command = cmd(1, 1, 1, 1);
        command.market_id = None;
        command.route_epoch = None;
        command.payload = Command::ShardKillSwitch(matching_domain::ShardKillSwitch {
            target_shard_id: matching_domain::ShardId(1),
            parent_command_id: CommandId(7),
            expected_primary_epoch: 1,
            active: true,
            cancel_all: true,
        });
        let first = Intent::from(&command);
        if let Command::ShardKillSwitch(ref mut value) = command.payload {
            value.expected_primary_epoch = 2;
        }
        assert_eq!(first, Intent::from(&command));
        if let Command::ShardKillSwitch(ref mut value) = command.payload {
            value.cancel_all = false;
        }
        assert_ne!(first, Intent::from(&command));
    }
    #[test]
    fn migration_retains_original_seq_and_alias_without_advancing_target() {
        use matching_domain::{
            ShardId,
            snapshot_wire::{SnapshotDecoder, SnapshotEncoder},
        };
        let cfg = IdempotencyConfig {
            command_capacity: 3,
            session_capacity: 3,
            aliases_per_session: 3,
        };
        let mut source = DedupWindow::new(cfg).unwrap();
        let mut target = DedupWindow::new(cfg).unwrap();
        let old = cmd(1, 10, 3, 5);
        source.insert(&old, &result(1, 100000));
        target.insert(&cmd(2, 11, 4, 6), &result(2, 1));
        source
            .transfer_market_to(&mut target, MarketId(5), ShardId(1))
            .unwrap();
        assert!(source.lookup(&old).unwrap().is_none());
        let mut w = SnapshotEncoder::new(65536);
        target.write_snapshot(&mut w).unwrap();
        let bytes = w.finish();
        let mut r = SnapshotDecoder::new(&bytes, 65536).unwrap();
        let restored = DedupWindow::read_snapshot(&mut r, cfg, 1).unwrap();
        assert_eq!(restored.lookup(&old).unwrap().unwrap().seq, 100000);
        assert!(restored.lookup(&cmd(9, 10, 3, 5)).is_err());
    }

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
            market_data_child_opens: Vec::new(),
            market_data_open_details: Vec::new(),
            market_data_source: Default::default(),
            deduplicated: false,
        }
    }
    /// 以持久窗口原格式逐项构造控制命令、返回类型和事件范围，保留有效旧下限结果。
    #[test]
    fn control_snapshot_binds_action_outcome_and_event_range() {
        use matching_domain::{
            ApplyGlobalControlFenceShard, ApplyUserControlFenceShard, UserFenceAction,
            snapshot_wire::{SnapshotDecoder, SnapshotEncoder},
        };
        let cfg = IdempotencyConfig {
            command_capacity: 1,
            session_capacity: 1,
            aliases_per_session: 1,
        };
        let fence = UserFenceAction::FenceOnly;
        let cancel = UserFenceAction::FenceAndCancelAll;
        let keep = UserFenceAction::Kill {
            active: true,
            cancel_all: false,
        };
        for (user, action, tag, value, last, valid) in [
            (true, cancel, 1, 1, 0, false),
            (false, fence, 2, 0, 0, false),
            (true, fence, 2, 0, 0, false),
            (true, fence, 1, 1, 0, true),
            (true, fence, 1, 3, 0, true),
            (true, fence, 1, 0, 0, false),
            (true, fence, 1, 1, 1, false),
            (false, keep, 2, 1, 1, false),
            (false, keep, 2, 0, 1, true),
            (true, cancel, 2, 1, 2, true),
            (false, cancel, 2, 1, 3, false),
        ] {
            let mut c = cmd(1, 0, 0, 0);
            c.route_epoch = None;
            c.market_id = None;
            c.user_id = user.then_some(UserId(10));
            c.payload = if user {
                Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
                    parent_command_id: CommandId(2),
                    user_id: UserId(10),
                    new_epoch: 1,
                    action,
                    expected_primary_epoch: 1,
                })
            } else {
                Command::ApplyGlobalControlFenceShard(ApplyGlobalControlFenceShard {
                    parent_command_id: CommandId(2),
                    new_epoch: 1,
                    action,
                    expected_primary_epoch: 1,
                })
            };
            let mut body = SnapshotEncoder::new(512);
            for _ in 0..3 {
                body.u64(1).unwrap();
            }
            body.u32(1).unwrap();
            body.bytes(&matching_domain::encode_wal_command(&c).unwrap())
                .unwrap();
            body.u64(1).unwrap();
            body.u8(tag).unwrap();
            body.u64(value).unwrap();
            body.u8(1).unwrap();
            body.u32(0).unwrap();
            body.u32(last).unwrap();
            body.u8(0).unwrap();
            body.u32(0).unwrap();
            let mut bytes = SnapshotEncoder::new(512);
            bytes.bytes(&body.finish()).unwrap();
            let raw = bytes.finish();
            let mut reader = SnapshotDecoder::new(&raw, 512).unwrap();
            assert_eq!(
                DedupWindow::read_snapshot(&mut reader, cfg, 1).is_ok(),
                valid,
                "{user}, {action:?}, {tag}, {value}, {last}"
            );
        }
    }

    #[test]
    fn snapshot_restores_successful_market_management_dedup() {
        use matching_domain::{
            Command, StartSession,
            snapshot_wire::{SnapshotDecoder, SnapshotEncoder},
        };
        let config = IdempotencyConfig::default();
        let mut window = DedupWindow::new(config).unwrap();
        let mut command = cmd(1, 1, 1, 7);
        command.payload = Command::StartSession(StartSession {
            market_id: MarketId(7),
            new_session_id: 8,
        });
        let mut outcome = result(1, 1);
        outcome.outcome = FormalOutcome::MarketManaged;
        window.insert(&command, &outcome);
        let mut encoder = SnapshotEncoder::new(4096);
        window.write_snapshot(&mut encoder).unwrap();
        let bytes = encoder.finish();
        let mut decoder = SnapshotDecoder::new(&bytes, 4096).unwrap();
        let restored = DedupWindow::read_snapshot(&mut decoder, config, 1).unwrap();
        decoder.finish().unwrap();
        let replay = restored.lookup(&command).unwrap().unwrap();
        assert!(replay.deduplicated);
        assert_eq!(replay.outcome, FormalOutcome::MarketManaged);
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

    #[test]
    fn snapshot_restores_cached_results_aliases_and_eviction_order() {
        use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotEncoder};
        let config = IdempotencyConfig {
            command_capacity: 2,
            session_capacity: 2,
            aliases_per_session: 2,
        };
        let mut d = DedupWindow::new(config).unwrap();
        let a = cmd(1, 1, 1, 7);
        let b = cmd(2, 1, 2, 7);
        d.insert(&a, &result(1, 1));
        d.insert(&b, &result(2, 2));
        let mut retry = a.clone();
        retry.session_id = 2;
        retry.client_seq = 9;
        d.remember_alias(&retry);
        let mut w = SnapshotEncoder::new(10000);
        d.write_snapshot(&mut w).unwrap();
        let bytes = w.finish();
        let mut r = SnapshotDecoder::new(&bytes, 10000).unwrap();
        let mut restored = DedupWindow::read_snapshot(&mut r, config, 2).unwrap();
        r.finish().unwrap();
        let actual = restored.lookup(&retry).unwrap().unwrap();
        let expected = d.lookup(&retry).unwrap().unwrap();
        assert_eq!(actual.seq, expected.seq);
        assert_eq!(actual.outcome, expected.outcome);
        assert_eq!(actual.summary, expected.summary);
        assert_eq!(actual.event_range, expected.event_range);
        let collision = cmd(9, 2, 9, 7);
        assert!(restored.lookup(&collision).is_err());
        let c = cmd(3, 1, 3, 7);
        restored.insert(&c, &result(3, 3));
        assert!(restored.lookup(&a).unwrap().is_none());
        assert!(restored.lookup(&b).unwrap().is_some());
        for end in 0..bytes.len() {
            let mut r = SnapshotDecoder::new(&bytes[..end], 10000).unwrap();
            assert!(DedupWindow::read_snapshot(&mut r, config, 2).is_err());
        }
    }
    #[test]
    fn snapshot_rejects_result_incompatible_with_original_command() {
        use matching_domain::snapshot_wire::SnapshotEncoder;
        let mut window = DedupWindow::new(IdempotencyConfig::default()).unwrap();
        let command = cmd(1, 1, 1, 7);
        let mut wrong = result(1, 1);
        wrong.outcome = FormalOutcome::UserFence(9);
        window.insert(&command, &wrong);
        let mut writer = SnapshotEncoder::new(10000);
        assert!(window.write_snapshot(&mut writer).is_err());
    }
    #[test]
    fn canonical_state_ignores_physical_session_slots() {
        use matching_domain::snapshot_wire::SnapshotEncoder;
        let config = IdempotencyConfig {
            command_capacity: 2,
            session_capacity: 2,
            aliases_per_session: 2,
        };
        let mut first = DedupWindow::new(config).unwrap();
        let a = cmd(1, 1, 1, 7);
        let b = cmd(2, 2, 2, 7);
        first.insert(&a, &result(1, 1));
        first.insert(&b, &result(2, 2));
        let mut second = first.try_clone_preallocated().unwrap();
        second.sessions.swap(0, 1);
        second.session_order.clear();
        second.session_order.extend([1, 0]);
        let mut a = SnapshotEncoder::new(10000);
        let mut b = SnapshotEncoder::new(10000);
        first.write_canonical_state(&mut a).unwrap();
        second.write_canonical_state(&mut b).unwrap();
        assert_eq!(a.finish(), b.finish());
    }
}
