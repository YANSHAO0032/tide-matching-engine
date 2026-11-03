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
