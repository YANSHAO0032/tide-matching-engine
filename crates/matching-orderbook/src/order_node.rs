use crate::OrderIndex;
use matching_domain::{LimitGtcOrder, QueuePriority};

/// Arena 中的订单节点。
///
/// 保存原始订单、剩余数量、队列优先级及双向链表链接。
/// 链接使用 Option<OrderIndex>，不保留特殊空值。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderNode {
    /// 创建时的不可变订单请求；运行态链接和剩余量不回写该值。
    order: LimitGtcOrder,
    /// 当前仍显示在本档位的正数量，初始化为原始订单数量。
    remaining: u64,
    /// 当前 market-local 排队优先级；链表链接才是 FIFO 权威顺序。
    priority: QueuePriority,
    /// 同一 `PriceLevel` 中的前驱槽位；头节点为 `None`。
    prev: Option<OrderIndex>,
    /// 同一 `PriceLevel` 中的后继槽位；尾节点为 `None`。
    next: Option<OrderIndex>,
}

impl OrderNode {
    /// 从原始 Limit GTC 订单创建尚未链接的运行态节点。
    ///
    /// `remaining` 精确复制原始 `Qty`，两个链接均为空；只有 `PriceLevel`
    /// 在完成局部不变量预检后才可连接该节点。
    pub fn new(order: LimitGtcOrder, priority: QueuePriority) -> Self {
        let remaining = order.qty.get();
        OrderNode {
            order,
            remaining,
            priority,
            prev: None,
            next: None,
        }
    }

    /// 借用创建此节点的不可变订单数据。
    pub fn original_order(&self) -> &LimitGtcOrder {
        &self.order
    }

    /// 返回当前运行态可见剩余量，不改变节点或订单数量。
    pub fn remaining(&self) -> u64 {
        self.remaining
    }

    /// 返回 market-local 排队优先级；它不替代链表定义的 FIFO 顺序。
    pub fn priority(&self) -> QueuePriority {
        self.priority
    }

    /// 返回本档位前驱槽位；不能据此推断槽位仍在 Arena 中存活。
    pub fn prev(&self) -> Option<OrderIndex> {
        self.prev
    }

    /// 返回本档位后继槽位；不能据此验证整条链或成员归属。
    pub fn next(&self) -> Option<OrderIndex> {
        self.next
    }

    /// 仅由完成局部链表预检的 crate 内维护路径设置前驱。
    ///
    /// 单独调用不会验证双向链接、side/price 或 priority 不变量。
    pub(crate) fn set_prev(&mut self, prev: Option<OrderIndex>) {
        self.prev = prev;
    }

    /// 仅由完成局部链表预检的 crate 内维护路径设置后继。
    ///
    /// 单独调用不会验证双向链接、side/price 或 priority 不变量。
    pub(crate) fn set_next(&mut self, next: Option<OrderIndex>) {
        self.next = next;
    }
}

#[cfg(test)]
mod tests {
    //! 新节点未链接初始化与完整字段保留的覆盖。

    use super::*;
    use matching_domain::{OrderId, Price, Qty, Side, UserId};

    #[test]
    fn new_initializes_all_fields() {
        let cases = [
            (1, Side::Buy, 10, 7),
            (2, Side::Sell, 25, 8),
            (3, Side::Buy, u64::MAX, 9),
            (4, Side::Sell, u64::MAX, 10),
        ];

        for (id, side, qty, priority) in cases {
            let original = LimitGtcOrder {
                order_id: OrderId::new(id),
                user_id: UserId::new(id as u64),
                side,
                price: Price::try_new(100).unwrap(),
                qty: Qty::try_new(qty).unwrap(),
            };

            let priority = QueuePriority::new(priority);
            let node = OrderNode::new(original.clone(), priority);

            assert_eq!(node.original_order(), &original);
            assert_eq!(node.remaining(), qty);
            assert_eq!(node.priority(), priority);
            assert_eq!(node.prev(), None);
            assert_eq!(node.next(), None);
        }
    }
}
