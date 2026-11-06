//! 客户端订单身份的分片内活动索引及确定性终结保留。

use matching_domain::{MarketId, OrderId, RejectReason, UserId};
use std::collections::VecDeque;

/// 已接受订单绑定的客户端身份。
#[derive(Clone, Debug, PartialEq, Eq)]
struct ClientOrderKey {
    /// 身份所属用户。
    user: UserId,
    /// 用户提供的客户端身份。
    client_id: String,
    /// 引擎订单身份。
    order_id: OrderId,
    /// 订单所属市场，终结处理使用市场与订单身份共同定位。
    market: MarketId,
}

/// 活动订单容量及终结历史分别受限，活动项不参与历史淘汰。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClientOrderIds {
    /// 活动索引的最大数量，与实际活动订单上限相同。
    active_capacity: usize,
    /// 活动订单索引。
    active: Vec<ClientOrderKey>,
    /// 按实际终结次序保存的有限历史。
    terminal: VecDeque<ClientOrderKey>,
    /// 历史保留上限。
    terminal_capacity: usize,
    /// 单个客户端身份允许的最大 UTF-8 字节数。
    max_id_bytes: usize,
}
impl ClientOrderIds {
    /// 活动身份按用户及客户端身份排序，终结历史保持淘汰顺序。
    pub(crate) fn write_canonical_state(
        &self,
        writer: &mut matching_domain::snapshot_wire::SnapshotEncoder,
    ) -> Result<(), matching_domain::snapshot_wire::SnapshotWireError> {
        let mut sorted = self
            .try_clone_preallocated()
            .map_err(|_| matching_domain::snapshot_wire::SnapshotWireError)?;
        sorted
            .active
            .sort_by(|a, b| (a.user, &a.client_id).cmp(&(b.user, &b.client_id)));
        sorted.write_snapshot(writer)
    }
    /// 对每个活动客户端身份核验其对应的真实订单。
    pub(crate) fn validate_active(
        &self,
        mut verify: impl FnMut(MarketId, OrderId, UserId) -> bool,
    ) -> bool {
        self.active
            .iter()
            .all(|key| verify(key.market, key.order_id, key.user))
    }
    /// 保存容量、活动身份和按终结顺序排列的历史。
    pub(crate) fn write_snapshot(
        &self,
        writer: &mut matching_domain::snapshot_wire::SnapshotEncoder,
    ) -> Result<(), matching_domain::snapshot_wire::SnapshotWireError> {
        use matching_domain::snapshot_wire::{SnapshotEncoder, SnapshotWireError};
        let mut w = SnapshotEncoder::new(256 * 1024 * 1024);
        for value in [
            self.active_capacity,
            self.terminal_capacity,
            self.max_id_bytes,
        ] {
            w.u64(u64::try_from(value).map_err(|_| SnapshotWireError)?)?;
        }
        w.u32(u32::try_from(self.active.len()).map_err(|_| SnapshotWireError)?)?;
        w.u32(u32::try_from(self.terminal.len()).map_err(|_| SnapshotWireError)?)?;
        for key in self.active.iter().chain(self.terminal.iter()) {
            w.u64(key.user.get())?;
            w.bytes(key.client_id.as_bytes())?;
            w.u128(key.order_id.get())?;
            w.u32(key.market.get())?;
        }
        writer.bytes(&w.finish())
    }
    /// 使用可信容量恢复索引，拒绝身份重复、空值和非法 UTF-8。
    pub(crate) fn read_snapshot(
        reader: &mut matching_domain::snapshot_wire::SnapshotDecoder<'_>,
        active: usize,
        terminal: usize,
        max_bytes: usize,
    ) -> Result<Self, matching_domain::snapshot_wire::SnapshotWireError> {
        use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotWireError};
        let bytes = reader.bytes()?;
        let mut r = SnapshotDecoder::new(bytes, 256 * 1024 * 1024)?;
        for expected in [active, terminal, max_bytes] {
            if usize::try_from(r.u64()?).ok() != Some(expected) {
                return Err(SnapshotWireError);
            }
        }
        let active_count = usize::try_from(r.u32()?).map_err(|_| SnapshotWireError)?;
        let terminal_count = usize::try_from(r.u32()?).map_err(|_| SnapshotWireError)?;
        let count = active_count
            .checked_add(terminal_count)
            .ok_or(SnapshotWireError)?;
        if active_count > active || terminal_count > terminal || count > r.remaining() / 32 {
            return Err(SnapshotWireError);
        }
        let mut index =
            Self::with_max_id_bytes(active, terminal, max_bytes).map_err(|_| SnapshotWireError)?;
        for position in 0..count {
            let user = UserId::new(r.u64()?);
            let data = r.bytes()?;
            if data.is_empty() || data.len() > max_bytes {
                return Err(SnapshotWireError);
            }
            let text = std::str::from_utf8(data).map_err(|_| SnapshotWireError)?;
            if index.lookup(user, text).is_some() {
                return Err(SnapshotWireError);
            }
            let mut client_id = String::new();
            client_id
                .try_reserve_exact(data.len())
                .map_err(|_| SnapshotWireError)?;
            client_id.push_str(text);
            let order_id = OrderId::new(r.u128()?);
            let market = MarketId::new(r.u32()?);
            if position < active_count
                && index
                    .active
                    .iter()
                    .any(|key| key.market == market && key.order_id == order_id)
            {
                return Err(SnapshotWireError);
            }
            let key = ClientOrderKey {
                user,
                client_id,
                order_id,
                market,
            };
            if position < active_count {
                index.active.push(key)
            } else {
                index.terminal.push_back(key)
            }
        }
        r.finish()?;
        Ok(index)
    }
    /// 测试中使用默认身份长度上限构造索引。
    #[cfg(test)]
    pub(crate) fn new(active_capacity: usize, terminal_capacity: usize) -> Result<Self, ()> {
        Self::with_max_id_bytes(active_capacity, terminal_capacity, 128)
    }
    /// 使用显式身份长度上限预分配索引。
    pub(crate) fn with_max_id_bytes(
        active_capacity: usize,
        terminal_capacity: usize,
        max_id_bytes: usize,
    ) -> Result<Self, ()> {
        if active_capacity == 0 || max_id_bytes == 0 {
            return Err(());
        }
        let mut active = Vec::new();
        let mut terminal = VecDeque::new();
        active.try_reserve_exact(active_capacity).map_err(|_| ())?;
        terminal
            .try_reserve_exact(terminal_capacity)
            .map_err(|_| ())?;
        Ok(Self {
            active_capacity,
            active,
            terminal,
            terminal_capacity,
            max_id_bytes,
        })
    }
    /// 在提交前复制索引，并预留后续终结及接收所需的全部槽位。
    pub(crate) fn try_clone_preallocated(&self) -> Result<Self, ()> {
        let mut candidate = Self::with_max_id_bytes(
            self.active_capacity,
            self.terminal_capacity,
            self.max_id_bytes,
        )?;
        candidate.active.extend(self.active.iter().cloned());
        candidate.terminal.extend(self.terminal.iter().cloned());
        Ok(candidate)
    }
    /// 按用户隔离检查活动和保留的终结身份。
    pub(crate) fn lookup(&self, user: UserId, client_id: &str) -> Option<(MarketId, OrderId)> {
        self.active
            .iter()
            .chain(self.terminal.iter())
            .find(|key| key.user == user && key.client_id == client_id)
            .map(|key| (key.market, key.order_id))
    }
    /// 仅在候选状态接受订单，调用方负责同时执行真实活动订单容量检查。
    pub(crate) fn accept_in_market(
        &mut self,
        market: MarketId,
        user: UserId,
        client_id: String,
        order_id: OrderId,
    ) -> Result<(), RejectReason> {
        if client_id.is_empty() || client_id.len() > self.max_id_bytes {
            return Err(RejectReason::InvalidOrderCombination);
        }
        if self.lookup(user, &client_id).is_some() {
            return Err(RejectReason::DuplicateClientOrderId);
        }
        if self.active.len() == self.active_capacity {
            return Err(RejectReason::EngineBusy);
        }
        self.active.push(ClientOrderKey {
            user,
            client_id,
            order_id,
            market,
        });
        Ok(())
    }
    /// 订单终结后转入有界历史，按终结先后淘汰最早条目。
    pub(crate) fn terminal_in_market(&mut self, market: MarketId, order_id: OrderId) {
        if let Some(position) = self
            .active
            .iter()
            .position(|key| key.market == market && key.order_id == order_id)
        {
            let key = self.active.remove(position);
            if self.terminal_capacity > 0 {
                if self.terminal.len() == self.terminal_capacity {
                    self.terminal.pop_front();
                }
                self.terminal.push_back(key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use matching_domain::{OrderId, RejectReason, UserId};
    #[test]
    fn canonical_state_sorts_active_keys_and_keeps_terminal_fifo() {
        use matching_domain::snapshot_wire::SnapshotEncoder;
        let mut a = ClientOrderIds::new(3, 2).unwrap();
        let mut b = ClientOrderIds::new(3, 2).unwrap();
        a.accept_in_market(MarketId(1), UserId(1), "a".into(), OrderId(1))
            .unwrap();
        a.accept_in_market(MarketId(1), UserId(2), "b".into(), OrderId(2))
            .unwrap();
        b.accept_in_market(MarketId(1), UserId(2), "b".into(), OrderId(2))
            .unwrap();
        b.accept_in_market(MarketId(1), UserId(1), "a".into(), OrderId(1))
            .unwrap();
        let mut left = SnapshotEncoder::new(1000);
        let mut right = SnapshotEncoder::new(1000);
        a.write_canonical_state(&mut left).unwrap();
        b.write_canonical_state(&mut right).unwrap();
        assert_eq!(left.finish(), right.finish());
    }

    #[test]
    fn snapshot_preserves_active_and_terminal_fifo_with_trusted_limits() {
        use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotEncoder};
        let mut ids = ClientOrderIds::with_max_id_bytes(3, 2, 16).unwrap();
        for (id, name) in [(1, "甲"), (2, "b"), (3, "c")] {
            ids.accept_in_market(
                MarketId(7),
                UserId(id),
                name.into(),
                OrderId(u128::from(id)),
            )
            .unwrap();
        }
        ids.terminal_in_market(MarketId(7), OrderId(1));
        ids.terminal_in_market(MarketId(7), OrderId(2));
        let mut w = SnapshotEncoder::new(1000);
        ids.write_snapshot(&mut w).unwrap();
        let bytes = w.finish();
        let mut r = SnapshotDecoder::new(&bytes, 1000).unwrap();
        let mut restored = ClientOrderIds::read_snapshot(&mut r, 3, 2, 16).unwrap();
        r.finish().unwrap();
        assert_eq!(restored, ids);
        restored.terminal_in_market(MarketId(7), OrderId(3));
        assert_eq!(restored.lookup(UserId(1), "甲"), None);
        assert!(restored.lookup(UserId(2), "b").is_some());
        let mut r = SnapshotDecoder::new(&bytes, 1000).unwrap();
        assert!(ClientOrderIds::read_snapshot(&mut r, 2, 2, 16).is_err());
        for end in 0..bytes.len() {
            let mut r = SnapshotDecoder::new(&bytes[..end], 1000).unwrap();
            assert!(ClientOrderIds::read_snapshot(&mut r, 3, 2, 16).is_err());
        }
    }

    #[test]
    fn active_keys_outlive_terminal_fifo_and_users_are_isolated() {
        let mut index = ClientOrderIds::new(3, 1).unwrap();
        index
            .accept_in_market(MarketId(1), UserId(1), "a".into(), OrderId(1))
            .unwrap();
        index
            .accept_in_market(MarketId(1), UserId(2), "a".into(), OrderId(2))
            .unwrap();
        assert_eq!(
            index.accept_in_market(MarketId(1), UserId(1), "a".into(), OrderId(3)),
            Err(RejectReason::DuplicateClientOrderId)
        );
        index.terminal_in_market(MarketId(1), OrderId(1));
        index
            .accept_in_market(MarketId(1), UserId(1), "b".into(), OrderId(3))
            .unwrap();
        index.terminal_in_market(MarketId(1), OrderId(3));
        assert_eq!(index.lookup(UserId(1), "a"), None);
        assert_eq!(
            index.lookup(UserId(1), "b"),
            Some((MarketId(1), OrderId(3)))
        );
        assert_eq!(
            index.lookup(UserId(2), "a"),
            Some((MarketId(1), OrderId(2)))
        );
    }

    #[test]
    fn active_capacity_rejects_without_evicting_and_terminal_retention_can_be_disabled() {
        let mut index = ClientOrderIds::new(1, 0).unwrap();
        index
            .accept_in_market(MarketId(1), UserId(1), "a".into(), OrderId(1))
            .unwrap();
        assert_eq!(
            index.accept_in_market(MarketId(1), UserId(1), "b".into(), OrderId(2)),
            Err(RejectReason::EngineBusy)
        );
        assert_eq!(
            index.lookup(UserId(1), "a"),
            Some((MarketId(1), OrderId(1)))
        );
        index.terminal_in_market(MarketId(1), OrderId(1));
        assert_eq!(index.lookup(UserId(1), "a"), None);
        index
            .accept_in_market(MarketId(1), UserId(1), "b".into(), OrderId(2))
            .unwrap();
    }

    #[test]
    fn invalid_client_id_does_not_consume_active_capacity() {
        let mut index = ClientOrderIds::with_max_id_bytes(1, 2, 128).unwrap();
        assert_eq!(
            index.accept_in_market(MarketId(1), UserId(1), String::new(), OrderId(1)),
            Err(RejectReason::InvalidOrderCombination)
        );
        assert_eq!(
            index.accept_in_market(MarketId(1), UserId(1), "x".repeat(129), OrderId(1)),
            Err(RejectReason::InvalidOrderCombination)
        );
        index
            .accept_in_market(MarketId(1), UserId(1), "x".repeat(128), OrderId(1))
            .unwrap();
    }

    #[test]
    fn terminal_identity_is_scoped_by_market_when_order_ids_repeat() {
        let mut index = ClientOrderIds::new(2, 2).unwrap();
        index
            .accept_in_market(
                matching_domain::MarketId(1),
                UserId(1),
                "a".into(),
                OrderId(7),
            )
            .unwrap();
        index
            .accept_in_market(
                matching_domain::MarketId(2),
                UserId(2),
                "b".into(),
                OrderId(7),
            )
            .unwrap();
        index.terminal_in_market(matching_domain::MarketId(2), OrderId(7));
        assert_eq!(index.active.len(), 1);
        assert_eq!(index.active[0].user, UserId(1));
        assert_eq!(index.terminal[0].user, UserId(2));
    }

    #[test]
    fn candidate_copy_reserves_the_configured_capacity_before_commit() {
        let mut index = ClientOrderIds::with_max_id_bytes(4, 5, 16).unwrap();
        index
            .accept_in_market(MarketId(1), UserId(1), "a".into(), OrderId(1))
            .unwrap();
        let candidate = index.try_clone_preallocated().unwrap();
        assert!(candidate.active.capacity() >= 4);
        assert!(candidate.terminal.capacity() >= 5);
        assert_eq!(candidate, index);
    }
}
