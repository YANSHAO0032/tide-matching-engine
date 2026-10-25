use crate::OrderIndex;
use matching_domain::{IcebergOrder, LimitGtcOrder, Qty, QueuePriority, TimeInForce};

/// Arena 中的订单节点。
///
/// 保存原始订单、剩余数量、队列优先级及双向链表链接。
/// 链接使用 `Option<OrderIndex>`，不保留特殊空值。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderNode {
    /// 最近一次已接受的绝对总量载荷；成交和运行态链接不回写该值。
    order: LimitGtcOrder,
    /// 当前仍显示在本档位的正数量；冰山挂单只显示当前可见片。
    remaining: u64,
    /// 未计入公开深度、等待后续补量的数量；普通订单为零。
    hidden_remaining: u64,
    /// 冰山每次展示量的上限；普通订单为 None。
    peak_size: Option<Qty>,
    /// 当前市场内排队优先级；链表链接才是先进先出权威顺序。
    priority: QueuePriority,
    /// 普通挂单的存活期；既有入口始终使用 GTC。
    tif: TimeInForce,
    /// 同一 `PriceLevel` 中的前驱槽位；头节点为 `None`。
    prev: Option<OrderIndex>,
    /// 同一 `PriceLevel` 中的后继槽位；尾节点为 `None`。
    next: Option<OrderIndex>,
}

impl OrderNode {
    /// 从原始限价单 GTC 订单创建尚未链接的运行态节点。
    ///
    /// `remaining` 精确复制原始 `Qty`，两个链接均为空；只有 `PriceLevel`
    /// 在完成局部不变量预检后才可连接该节点。
    pub fn new(order: LimitGtcOrder, priority: QueuePriority) -> Self {
        Self::new_timed(order, priority, TimeInForce::Gtc)
    }

    /// 使用已验证有效期构造尚未链接的普通挂单运行态节点。
    pub(crate) fn new_timed(
        order: LimitGtcOrder,
        priority: QueuePriority,
        tif: TimeInForce,
    ) -> Self {
        let remaining = order.qty.get();
        OrderNode {
            order,
            remaining,
            hidden_remaining: 0,
            peak_size: None,
            priority,
            tif,
            prev: None,
            next: None,
        }
    }

    /// 根据冰山未成交总余量构造挂单的可见片。
    pub(crate) fn new_iceberg(
        iceberg: IcebergOrder,
        total_remaining: u64,
        priority: QueuePriority,
    ) -> Self {
        assert!(total_remaining > 0);
        let visible = total_remaining.min(iceberg.peak_size().get());
        let hidden = total_remaining
            .checked_sub(visible)
            .expect("visible <= total");
        Self {
            order: iceberg.order().clone(),
            remaining: visible,
            hidden_remaining: hidden,
            peak_size: Some(iceberg.peak_size()),
            priority,
            tif: TimeInForce::Gtc,
            prev: None,
            next: None,
        }
    }

    /// 借用最近一次已接受的订单载荷，数量包含已经成交的部分。
    pub fn original_order(&self) -> &LimitGtcOrder {
        &self.order
    }

    /// 返回当前运行态可见剩余量，不改变节点或订单数量。
    pub fn remaining(&self) -> u64 {
        self.remaining
    }

    /// 返回参与当前撮合与公开深度聚合的可见量。
    pub fn visible_remaining(&self) -> u64 {
        self.remaining
    }

    /// 返回尚未展示的余量，不计入公开深度。
    pub fn hidden_remaining(&self) -> u64 {
        self.hidden_remaining
    }

    /// 返回可见量与隐藏量之和，供取消和生命周期事件使用。
    pub fn total_remaining(&self) -> u64 {
        self.remaining
            .checked_add(self.hidden_remaining)
            .expect("iceberg total remaining overflow")
    }

    /// 返回冰山展示上限；普通订单返回 None。
    pub fn peak_size(&self) -> Option<Qty> {
        self.peak_size
    }

    /// 完成优先级预检后，替换已全部成交且已脱离链表的可见片。
    /// 调用方已从价档聚合中扣除旧可见量；节点仍保存该旧值，
    /// 直到此处用新可见片的数量替换。
    pub(crate) fn replenish(&mut self, priority: QueuePriority) -> Qty {
        assert!(self.remaining > 0 && self.hidden_remaining > 0 && self.peak_size.is_some());
        assert!(self.prev.is_none() && self.next.is_none());
        let visible = self
            .hidden_remaining
            .min(self.peak_size.expect("iceberg peak").get());
        self.hidden_remaining = self
            .hidden_remaining
            .checked_sub(visible)
            .expect("visible <= hidden");
        self.remaining = visible;
        self.priority = priority;
        Qty::try_new(visible).expect("replenished slice positive")
    }

    /// 提交已经完成全部外部预检的严格部分成交剩余量。
    ///
    /// 该方法只负责最终写入，不负责计算成交。
    /// 新剩余量必须保持正数且严格小于旧剩余量；
    /// 完整成交必须由 PriceLevel 的删除路径处理。
    pub(crate) fn commit_partial_remaining(&mut self, new_remaining: u64) {
        assert!(
            new_remaining > 0,
            "OrderNode invariant violation: partial fill exhausted order"
        );

        assert!(
            new_remaining < self.remaining,
            "OrderNode invariant violation: partial fill must reduce remaining"
        );

        self.remaining = new_remaining;
    }

    /// 返回市场内排队优先级；它不替代链表定义的先进先出顺序。
    pub fn priority(&self) -> QueuePriority {
        self.priority
    }

    /// 返回挂单的存活期元数据。
    pub fn time_in_force(&self) -> TimeInForce {
        self.tif
    }

    /// 在新到订单余量已按既有 GTC 路径完成挂单后写入其已预检的存活期。
    pub(crate) fn replace_initial_gtc_with_tif(&mut self, tif: TimeInForce) {
        assert_eq!(
            self.tif,
            TimeInForce::Gtc,
            "OrderNode invariant violation: only initial GTC metadata may be replaced"
        );
        self.tif = tif;
    }

    /// 在同价减量的已验证提交点原位替换普通订单载荷。
    ///
    /// 调用方必须先完成价格档聚合、到期索引和绝对总量的全部预检；
    /// 本方法不触碰链接或优先级，因而保留原有先进先出位置。
    pub(crate) fn amend_regular_in_place(
        &mut self,
        order: LimitGtcOrder,
        remaining: Qty,
        tif: TimeInForce,
    ) {
        assert!(
            self.peak_size.is_none(),
            "iceberg must not use regular amend"
        );
        assert_eq!(order.order_id, self.order.order_id, "amend changed OrderId");
        assert_eq!(order.side, self.order.side, "amend changed side");
        assert_eq!(
            order.price, self.order.price,
            "in-place amend changed price"
        );
        assert!(
            remaining.get() <= self.remaining,
            "in-place amend increased remaining"
        );
        self.order = order;
        self.remaining = remaining.get();
        self.tif = tif;
    }

    /// 返回本档位前驱槽位；不能据此推断槽位仍在 Arena 中存活。
    pub fn prev(&self) -> Option<OrderIndex> {
        self.prev
    }

    /// 返回本档位后继槽位；不能据此验证整条链或成员归属。
    pub fn next(&self) -> Option<OrderIndex> {
        self.next
    }

    /// 冰山同价非增暴露改单只写入已校验的显示片与隐藏片，保留链接及优先级。
    pub(crate) fn amend_iceberg_in_place(
        &mut self,
        order: LimitGtcOrder,
        total: Qty,
        visible: Qty,
        peak: Qty,
        tif: TimeInForce,
    ) {
        assert!(self.peak_size.is_some());
        assert_eq!(order.order_id, self.order.order_id);
        assert_eq!(order.price, self.order.price);
        assert!(visible.get() <= self.remaining && total.get() <= self.total_remaining());
        self.order = order;
        self.remaining = visible.get();
        self.hidden_remaining = total
            .get()
            .checked_sub(visible.get())
            .expect("planned visible bounded by total");
        self.peak_size = Some(peak);
        self.tif = tif;
    }

    /// 仅由完成局部链表预检的库内维护路径设置前驱。
    ///
    /// 单独调用不会验证双向链接、方向/价格或优先级不变量。
    pub(crate) fn set_prev(&mut self, prev: Option<OrderIndex>) {
        self.prev = prev;
    }

    /// 仅由完成局部链表预检的库内维护路径设置后继。
    ///
    /// 单独调用不会验证双向链接、方向/价格或优先级不变量。
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
    #[test]
    fn commit_partial_remaining_changes_only_remaining() {
        let original = LimitGtcOrder {
            order_id: OrderId::new(1),
            user_id: UserId::new(1),
            side: Side::Buy,
            price: Price::try_new(100).unwrap(),
            qty: Qty::try_new(10).unwrap(),
        };

        let priority = QueuePriority::new(7);
        let mut node = OrderNode::new(original.clone(), priority);

        node.commit_partial_remaining(6);

        assert_eq!(node.original_order(), &original);
        assert_eq!(node.remaining(), 6);
        assert_eq!(node.priority(), priority);
        assert_eq!(node.prev(), None);
        assert_eq!(node.next(), None);
    }

    #[test]
    fn iceberg_tracks_visible_hidden_and_total_independently() {
        use matching_domain::IcebergOrder;
        let original = LimitGtcOrder {
            order_id: OrderId::new(11),
            user_id: UserId::new(1),
            side: Side::Sell,
            price: Price::try_new(100).unwrap(),
            qty: Qty::try_new(10).unwrap(),
        };
        let iceberg = IcebergOrder::try_new(original.clone(), Qty::try_new(3).unwrap()).unwrap();
        let mut node = OrderNode::new_iceberg(iceberg, 10, QueuePriority::new(4));
        assert_eq!(node.remaining(), 3);
        assert_eq!(node.visible_remaining(), 3);
        assert_eq!(node.hidden_remaining(), 7);
        assert_eq!(node.total_remaining(), 10);
        assert_eq!(node.peak_size(), Some(Qty::try_new(3).unwrap()));
        node.commit_partial_remaining(1);
        assert_eq!(node.total_remaining(), 8);
        assert_eq!(node.hidden_remaining(), 7);
    }
}
