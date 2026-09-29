use crate::{IncomingOrder, ReferenceOrder};
use matching_domain::{
    LimitGtcOrder, OrderId, Price, Qty, QueuePriority, RejectReason, Side, TradeEvent, TradeId,
};

/// 为 priority 计数器保留不可分配的高位保护空间。
///
/// 到达该边界前必须拒绝新 resting order，避免计数器 wrap 后破坏 FIFO 语义。
const PRIORITY_GUARD: u128 = 1_024;

/// 单市场参考订单簿
///
/// orders 的原始顺序表示已知挂单先后。
#[derive(Debug, Default)]
pub struct ReferenceOrderBook {
    /// 挂单的权威存储顺序；同价 FIFO 的参考基线由该顺序加稳定排序得出。
    orders: Vec<ReferenceOrder>,
    /// 下一张成功 rest 的订单将取得的 market-local priority 值。
    next_priority: u128,
}

impl ReferenceOrderBook {
    /// 创建不含挂单且 priority 从零开始的单市场参考订单簿。
    pub fn new() -> Self {
        Self::default()
    }

    /// 将订单加入当前市场的参考订单簿。
    ///
    /// 校验顺序：
    /// 1. 拒绝重复订单 ID；
    /// 2. 检查 priority 高水位；
    /// 3. checked increment；
    /// 4. 创建运行态订单并追加到源 Vec。
    ///
    /// 失败时不修改现有订单、剩余量或 next_priority。
    pub fn rest(&mut self, order: LimitGtcOrder) -> Result<(), RejectReason> {
        // 必须先检查重复 ID，即使计数器已经耗尽也是如此。
        if self
            .orders
            .iter()
            .any(|existing| existing.original_order().order_id == order.order_id)
        {
            return Err(RejectReason::DuplicateOrderId);
        }

        if self.next_priority >= u128::MAX - PRIORITY_GUARD {
            return Err(RejectReason::PrioritySpaceExhaustion);
        }

        // 所有可能失败的检查均在修改状态之前执行。
        let next = self
            .next_priority
            .checked_add(1)
            .ok_or(RejectReason::PrioritySpaceExhaustion)?;

        let priority = QueuePriority::new(self.next_priority);
        let resting = ReferenceOrder::new(order, priority);

        self.orders.push(resting);
        self.next_priority = next;

        Ok(())
    }

    /// 返回指定侧按价格优先、同价 FIFO 排列的只读视图
    ///
    /// Buy: 价格降序
    /// Sell: 价格升序
    ///
    /// 使用稳定排序保留同价订单在源 Vec 中的顺序
    /// 排序仅作用于引用列表，不修改底层订单
    pub fn orders(&self, side: Side) -> Vec<&ReferenceOrder> {
        let mut result: Vec<_> = self
            .orders
            .iter()
            .filter(|order| order.original_order().side == side)
            .collect();
        result.sort_by(|a, b| {
            let a_price = a.original_order().price;
            let b_price = b.original_order().price;
            match side {
                Side::Buy => b_price.cmp(&a_price),
                Side::Sell => a_price.cmp(&b_price),
            }
        });
        result
    }

    /// 返回指定侧价格/FIFO 优先级最高的订单。
    ///
    pub fn best(&self, side: Side) -> Option<&ReferenceOrder> {
        self.orders(side).into_iter().next()
    }

    /// 返回对 `incoming` 价格可成交的最优 resting maker。
    ///
    /// 选择遵循对手方价格优先和同价 FIFO，并忽略已经耗尽的运行态订单。
    pub fn best_crossing_maker(&self, incoming: &LimitGtcOrder) -> Option<&ReferenceOrder> {
        self.best_crossing_maker_index(incoming)
            .map(|index| &self.orders[index])
    }

    /// 查找最佳可成交 maker 在源 `Vec` 中的索引。
    ///
    /// 该索引只在当前不可变借用期间有效，供后续规划和一次性执行定位同一 maker。
    pub(crate) fn best_crossing_maker_index(&self, incoming: &LimitGtcOrder) -> Option<usize> {
        let opposite_side = match incoming.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        let maker = self.orders(opposite_side).into_iter().find(|maker| {
            if maker.remaining() == 0 {
                return false;
            }
            let maker_price = maker.original_order().price;

            match incoming.side {
                Side::Buy => incoming.price >= maker_price,
                Side::Sell => incoming.price <= maker_price,
            }
        })?;
        self.orders
            .iter()
            .position(|source| std::ptr::eq(source, maker))
    }

    /// 规划下一笔成交，返回源 Vec 索引和成交数量。
    ///
    /// 不修改订单簿，索引仅供内部使用。
    fn plan_next_fill(
        &self,
        incoming: &IncomingOrder,
    ) -> Result<Option<(usize, Qty)>, RejectReason> {
        if incoming.remaining() == 0 {
            return Ok(None);
        }
        let Some(maker_index) = self.best_crossing_maker_index(incoming.original_order()) else {
            return Ok(None);
        };
        // 计算双方当前最多能够成交的数量
        let maker_remaining = self.orders[maker_index].remaining();
        let fill_lots = incoming.remaining().min(maker_remaining);

        let fill_qty = Qty::try_new(fill_lots)?;
        Ok(Some((maker_index, fill_qty)))
    }

    /// 返回下一笔成交的 maker 与数量，但不修改双方运行态。
    ///
    /// 与 [`Self::plan_next_fill`] 相同的价格/FIFO 选择逻辑被用于只读预览；
    /// 返回的引用在订单簿发生可变操作前有效。
    pub fn next_fill_plan(
        &self,
        incoming: &IncomingOrder,
    ) -> Result<Option<(&ReferenceOrder, Qty)>, RejectReason> {
        let Some((maker_index, fill_qty)) = self.plan_next_fill(incoming)? else {
            return Ok(None);
        };
        Ok(Some((&self.orders[maker_index], fill_qty)))
    }

    /// 执行至多一笔 next-fill，不生成事件，也不将 incoming 剩余量挂簿。
    ///
    /// 先规划并预检双方扣减，再修改运行态。
    /// 返回 maker 身份、maker 价格和成交量。
    pub fn execute_next_fill(
        &mut self,
        incoming: &mut IncomingOrder,
    ) -> Result<Option<(OrderId, Price, Qty)>, RejectReason> {
        let Some((maker_index, fill_qty)) = self.plan_next_fill(incoming)? else {
            return Ok(None);
        };
        let lots = fill_qty.get();
        let maker = &self.orders[maker_index];

        maker
            .remaining()
            .checked_sub(lots)
            .ok_or(RejectReason::ArithmeticOverflow)?;
        incoming
            .remaining()
            .checked_sub(lots)
            .ok_or(RejectReason::ArithmeticOverflow)?;

        let maker_id = maker.original_order().order_id;
        let maker_price = maker.original_order().price;

        self.orders[maker_index].apply_fill(fill_qty)?;
        incoming.apply_fill(fill_qty)?;

        if self.orders[maker_index].remaining() == 0 {
            self.orders.remove(maker_index);
        }

        Ok(Some((maker_id, maker_price, fill_qty)))
    }

    /// 持续执行单笔成交，直到 incoming 耗尽或不存在可成交 maker。
    ///
    /// 结果按实际成交顺序排列，价格取各 resting maker 的价格。
    /// 每笔成交沿用 execute_next_fill 的双侧预检；不生成事件，也不 rest 剩余 incoming。
    pub fn execute_fills(
        &mut self,
        incoming: &mut IncomingOrder,
    ) -> Result<Vec<(OrderId, Price, Qty)>, RejectReason> {
        let mut fills = Vec::new();
        while let Some(fill) = self.execute_next_fill(incoming)? {
            fills.push(fill)
        }
        Ok(fills)
    }

    /// 按 OrderId 取消一张 resting order。
    ///
    /// 订单不存在时返回 OrderNotFound，不修改订单簿。
    /// 成功时返回被移除的订单，不消耗或重排 priority。
    pub fn cancel(&mut self, order_id: OrderId) -> Result<ReferenceOrder, RejectReason> {
        let index = self
            .orders
            .iter()
            .position(|order| order.original_order().order_id == order_id)
            .ok_or(RejectReason::OrderNotFound)?;
        Ok(self.orders.remove(index))
    }

    /// 执行至多一笔真实成交，并转换为 TradeEvent。
    ///
    /// TradeId 由调用方提供；无可成交 maker 时返回 None。
    /// 不生成 EngineEvent，不写 WAL，不自动 rest。
    pub fn execute_next_fill_trade(
        &mut self,
        incoming: &mut IncomingOrder,
        trade_id: TradeId,
    ) -> Result<Option<TradeEvent>, RejectReason> {
        let maker_side = match incoming.original_order().side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        let taker_order_id = incoming.original_order().order_id;

        let Some((maker_order_id, price, qty)) = self.execute_next_fill(incoming)? else {
            return Ok(None);
        };

        Ok(Some(TradeEvent {
            trade_id,
            maker_order_id,
            taker_order_id,
            price,
            qty,
            maker_side,
        }))
    }
}

#[cfg(test)]
mod tests {
    //! 覆盖 T02 参考模型的排序、成交、取消和 priority 边界。
    use super::*;
    use matching_domain::{LimitGtcOrder, OrderId, Price, Qty, UserId};

    /// 构造带指定业务字段的 resting order fixture。
    fn order(id: u128, side: Side, price: i64, qty: u64) -> ReferenceOrder {
        ReferenceOrder::new(
            LimitGtcOrder {
                order_id: OrderId::new(id),
                user_id: UserId::new(id as u64),
                side,
                price: Price::try_new(price).unwrap(),
                qty: Qty::try_new(qty).unwrap(),
            },
            QueuePriority::new(0),
        )
    }

    /// 直接构造非空参考簿，仅用于表达需要特定源顺序的测试前置条件。
    fn book(orders: Vec<ReferenceOrder>) -> ReferenceOrderBook {
        ReferenceOrderBook {
            orders,
            next_priority: 0,
        }
    }

    /// 提取只读视图中的 order ID，便于断言显示顺序。
    fn ids(orders: &[&ReferenceOrder]) -> Vec<OrderId> {
        orders
            .iter()
            .map(|order| order.original_order().order_id)
            .collect()
    }

    #[test]
    fn empty_book_and_empty_side() {
        let empty = ReferenceOrderBook::new();

        assert!(empty.orders(Side::Buy).is_empty());
        assert!(empty.orders(Side::Sell).is_empty());
        assert!(empty.best(Side::Buy).is_none());
        assert!(empty.best(Side::Sell).is_none());

        let buy_only = book(vec![order(1, Side::Buy, 90, 10)]);
        assert!(buy_only.orders(Side::Sell).is_empty());
        assert!(buy_only.best(Side::Sell).is_none());

        let sell_only = book(vec![order(2, Side::Sell, 110, 10)]);
        assert!(sell_only.orders(Side::Buy).is_empty());
        assert!(sell_only.best(Side::Buy).is_none());
    }

    #[test]
    fn buy_and_sell_views_are_isolated_and_price_sorted() {
        // 最高 bid=95，最低 ask=101，不构成交叉盘口。
        let book = book(vec![
            order(1, Side::Buy, 90, 10),
            order(2, Side::Sell, 105, 10),
            order(3, Side::Buy, 95, 10),
            order(4, Side::Sell, 101, 10),
            order(5, Side::Buy, 92, 10),
            order(6, Side::Sell, 103, 10),
        ]);

        assert_eq!(
            ids(&book.orders(Side::Buy)),
            vec![OrderId::new(3), OrderId::new(5), OrderId::new(1)]
        );
        assert_eq!(
            ids(&book.orders(Side::Sell)),
            vec![OrderId::new(4), OrderId::new(6), OrderId::new(2)]
        );
    }

    #[test]
    fn buy_same_price_preserves_fifo_not_id_order() {
        let book = book(vec![
            order(30, Side::Buy, 90, 10),
            order(10, Side::Buy, 90, 10),
            order(20, Side::Buy, 90, 10),
        ]);

        assert_eq!(
            ids(&book.orders(Side::Buy)),
            vec![OrderId::new(30), OrderId::new(10), OrderId::new(20)]
        );
    }

    #[test]
    fn sell_same_price_preserves_fifo_not_id_order() {
        let book = book(vec![
            order(30, Side::Sell, 110, 10),
            order(10, Side::Sell, 110, 10),
            order(20, Side::Sell, 110, 10),
        ]);

        assert_eq!(
            ids(&book.orders(Side::Sell)),
            vec![OrderId::new(30), OrderId::new(10), OrderId::new(20)]
        );
    }

    #[test]
    fn later_better_price_precedes_earlier_worse_price() {
        let book = book(vec![
            order(1, Side::Buy, 90, 10),
            order(2, Side::Sell, 110, 10),
            order(3, Side::Buy, 95, 10),
            order(4, Side::Sell, 105, 10),
        ]);

        assert_eq!(
            ids(&book.orders(Side::Buy)),
            vec![OrderId::new(3), OrderId::new(1)]
        );
        assert_eq!(
            ids(&book.orders(Side::Sell)),
            vec![OrderId::new(4), OrderId::new(2)]
        );
    }

    #[test]
    fn price_boundaries_sort_correctly() {
        // 分别测试两个单侧簿，避免构造交叉盘口。
        let buys = book(vec![
            order(1, Side::Buy, 1, 10),
            order(2, Side::Buy, i64::MAX, 10),
        ]);
        assert_eq!(
            ids(&buys.orders(Side::Buy)),
            vec![OrderId::new(2), OrderId::new(1)]
        );

        let sells = book(vec![
            order(3, Side::Sell, i64::MAX, 10),
            order(4, Side::Sell, 1, 10),
        ]);
        assert_eq!(
            ids(&sells.orders(Side::Sell)),
            vec![OrderId::new(4), OrderId::new(3)]
        );
    }

    #[test]
    fn repeated_queries_do_not_mutate_source_or_remaining() {
        let mut first = order(30, Side::Buy, 90, 10);
        first.apply_fill(Qty::try_new(3).unwrap()).unwrap();

        let book = book(vec![
            first,
            order(10, Side::Sell, 110, 20),
            order(20, Side::Buy, 95, 30),
            order(40, Side::Sell, 105, 40),
        ]);

        // 记录源 Vec 的顺序、完整原始订单及运行态剩余量。
        let snapshot: Vec<_> = book
            .orders
            .iter()
            .map(|order| (order.original_order().clone(), order.remaining()))
            .collect();

        let first_buy = ids(&book.orders(Side::Buy));
        let first_sell = ids(&book.orders(Side::Sell));

        for _ in 0..3 {
            assert_eq!(ids(&book.orders(Side::Buy)), first_buy);
            assert_eq!(ids(&book.orders(Side::Sell)), first_sell);

            assert!(book.best(Side::Buy).is_some());
            assert!(book.best(Side::Sell).is_some());
        }

        let after: Vec<_> = book
            .orders
            .iter()
            .map(|order| (order.original_order().clone(), order.remaining()))
            .collect();

        assert_eq!(snapshot, after);
        assert_eq!(book.orders[0].remaining(), 7);
        assert_eq!(book.orders[0].original_order().qty.get(), 10);
    }

    /// 构造经公开 `rest` 路径写入参考簿的合法订单 fixture。
    fn candidate(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
        LimitGtcOrder {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            price: Price::try_new(price).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
        }
    }

    /// 保存完整可观察 resting 状态，用于失败原子性断言。
    fn snapshot(book: &ReferenceOrderBook) -> Vec<(LimitGtcOrder, u64, QueuePriority)> {
        book.orders
            .iter()
            .map(|order| {
                (
                    order.original_order().clone(),
                    order.remaining(),
                    order.priority(),
                )
            })
            .collect()
    }

    #[test]
    fn rest_assigns_monotonically_increasing_priorities() {
        let mut book = ReferenceOrderBook::new();

        for id in [30, 10, 20] {
            book.rest(candidate(id, Side::Buy, 90, 10)).unwrap();
        }

        let view = book.orders(Side::Buy);

        let actual: Vec<_> = view
            .iter()
            .map(|order| (order.original_order().order_id, order.priority().get()))
            .collect();

        assert_eq!(
            actual,
            vec![
                (OrderId::new(30), 0),
                (OrderId::new(10), 1),
                (OrderId::new(20), 2),
            ]
        );

        assert_eq!(book.next_priority, 3);
    }

    #[test]
    fn real_rest_preserves_price_priority_and_both_sides_fifo() {
        let mut book = ReferenceOrderBook::new();

        // 双侧盘口不交叉：最高 bid=95，最低 ask=105。
        for order in [
            candidate(30, Side::Buy, 90, 10),
            candidate(40, Side::Sell, 110, 10),
            candidate(10, Side::Buy, 90, 10),
            candidate(50, Side::Sell, 110, 10),
            candidate(20, Side::Buy, 90, 10),
            candidate(60, Side::Sell, 110, 10),
            // 后到的更优价格必须排在前面。
            candidate(70, Side::Buy, 95, 10),
            candidate(80, Side::Sell, 105, 10),
        ] {
            book.rest(order).unwrap();
        }

        assert_eq!(
            ids(&book.orders(Side::Buy)),
            vec![
                OrderId::new(70),
                OrderId::new(30),
                OrderId::new(10),
                OrderId::new(20),
            ]
        );

        assert_eq!(
            ids(&book.orders(Side::Sell)),
            vec![
                OrderId::new(80),
                OrderId::new(40),
                OrderId::new(50),
                OrderId::new(60),
            ]
        );

        assert_eq!(book.best(Side::Buy).unwrap().priority().get(), 6);
        assert_eq!(book.best(Side::Sell).unwrap().priority().get(), 7);
    }

    #[test]
    fn duplicate_order_id_does_not_mutate_book() {
        let mut book = ReferenceOrderBook::new();

        let original = candidate(1, Side::Buy, 90, 10);
        book.rest(original.clone()).unwrap();

        let before = snapshot(&book);
        let next_before = book.next_priority;

        // 同一订单 ID，即使方向、价格、数量不同也必须拒绝。
        let result = book.rest(candidate(1, Side::Sell, 110, 20));

        assert_eq!(result, Err(RejectReason::DuplicateOrderId));
        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, next_before);
        assert_eq!(book.orders[0].original_order(), &original);
        assert_eq!(book.orders[0].remaining(), 10);

        // 失败不消耗 priority，下一张合法订单获得紧接着的值。
        book.rest(candidate(2, Side::Buy, 90, 10)).unwrap();

        assert_eq!(book.orders[1].priority().get(), 1);
        assert_eq!(book.next_priority, 2);
    }

    #[test]
    fn priority_guard_allows_last_safe_rest_then_rejects() {
        let threshold = u128::MAX - PRIORITY_GUARD;

        // 只在测试模块中初始化高水位计数器。
        let mut book = ReferenceOrderBook {
            orders: Vec::new(),
            next_priority: threshold - 1,
        };

        // 临界点前的最后一次成功挂单。
        book.rest(candidate(1, Side::Buy, 90, 10)).unwrap();

        assert_eq!(book.orders[0].priority().get(), threshold - 1);
        assert_eq!(book.next_priority, threshold);

        let before = snapshot(&book);

        // 达到阈值后，不能再消耗 priority。
        assert_eq!(
            book.rest(candidate(2, Side::Buy, 90, 10)),
            Err(RejectReason::PrioritySpaceExhaustion)
        );

        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, threshold);
    }

    #[test]
    fn duplicate_id_takes_precedence_over_priority_exhaustion() {
        let threshold = u128::MAX - PRIORITY_GUARD;

        let mut book = ReferenceOrderBook {
            orders: Vec::new(),
            next_priority: threshold - 1,
        };

        book.rest(candidate(1, Side::Buy, 90, 10)).unwrap();

        let before = snapshot(&book);

        // 同时满足重复 ID 和高水位耗尽时，必须先返回 2008。
        assert_eq!(
            book.rest(candidate(1, Side::Buy, 90, 10)),
            Err(RejectReason::DuplicateOrderId)
        );

        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, threshold);
    }

    #[test]
    fn failed_rest_preserves_existing_partial_fill() {
        let mut book = ReferenceOrderBook::new();

        book.rest(candidate(1, Side::Buy, 90, 10)).unwrap();

        book.orders[0].apply_fill(Qty::try_new(3).unwrap()).unwrap();

        let before = snapshot(&book);
        let next_before = book.next_priority;

        assert_eq!(
            book.rest(candidate(1, Side::Buy, 90, 10)),
            Err(RejectReason::DuplicateOrderId)
        );

        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, next_before);
        assert_eq!(book.orders[0].remaining(), 7);
        assert_eq!(book.orders[0].original_order().qty.get(), 10);
    }
    #[test]
    fn cancel_existing_order_preserves_other_orders() {
        let mut book = ReferenceOrderBook::new();

        for id in [30, 10, 20] {
            book.rest(candidate(id, Side::Buy, 95, 10)).unwrap();
        }

        // 预先部分成交待撤订单。
        book.orders[1].apply_fill(Qty::try_new(3).unwrap()).unwrap();

        let before = snapshot(&book);
        let priority_before = book.next_priority;

        let canceled = book.cancel(OrderId::new(10)).unwrap();

        assert_eq!(canceled.original_order().order_id, OrderId::new(10));
        assert_eq!(canceled.remaining(), 7);
        assert_eq!(canceled.priority(), before[1].2);

        // 其余订单保持原有 FIFO 顺序及运行态。
        assert_eq!(snapshot(&book), vec![before[0].clone(), before[2].clone()]);
        assert_eq!(book.next_priority, priority_before);

        // 撤销后允许相同 OrderId 再次挂入。
        book.rest(candidate(10, Side::Buy, 95, 10)).unwrap();

        assert_eq!(book.orders[2].priority().get(), priority_before);
        assert_eq!(book.next_priority, priority_before + 1);
    }

    #[test]
    fn cancel_missing_order_is_failure_atomic() {
        let mut book = ReferenceOrderBook::new();

        book.rest(candidate(1, Side::Sell, 105, 10)).unwrap();
        book.rest(candidate(2, Side::Sell, 105, 10)).unwrap();

        // 包含部分成交 maker，防止只验证原始订单。
        book.orders[0].apply_fill(Qty::try_new(4).unwrap()).unwrap();

        let before = snapshot(&book);
        let priority_before = book.next_priority;

        assert!(matches!(
            book.cancel(OrderId::new(999)),
            Err(RejectReason::OrderNotFound)
        ));

        // 完整原始订单、remaining、priority、源顺序不变。
        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, priority_before);

        // 失败后仍能按原有 FIFO 选择 maker。
        let best = book.best(Side::Sell).unwrap();
        assert_eq!(best.original_order().order_id, OrderId::new(1));
        assert_eq!(best.remaining(), 6);
    }

    #[test]
    fn cancel_empty_book_is_failure_atomic() {
        let mut book = ReferenceOrderBook::new();

        let before = snapshot(&book);
        let priority_before = book.next_priority;

        assert!(matches!(
            book.cancel(OrderId::new(1)),
            Err(RejectReason::OrderNotFound)
        ));

        assert_eq!(snapshot(&book), before);
        assert!(book.orders.is_empty());
        assert_eq!(book.next_priority, priority_before);
    }
    #[test]
    fn cancel_preserves_fifo_and_remaining_on_both_sides() {
        for side in [Side::Buy, Side::Sell] {
            // 分别取消同价 FIFO 队列的首项、中间项和尾项。
            for cancel_index in 0..3 {
                let mut book = ReferenceOrderBook::new();

                // ID 顺序故意不同于挂单顺序。
                for id in [30, 10, 20] {
                    book.rest(candidate(id, side, 100, 10)).unwrap();
                }

                // 制造不同的运行态剩余量：
                // ID=30 -> 9
                // ID=10 -> 7
                // ID=20 -> 5
                for (index, filled) in [1, 3, 5].into_iter().enumerate() {
                    book.orders[index]
                        .apply_fill(Qty::try_new(filled).unwrap())
                        .unwrap();
                }

                let before = snapshot(&book);
                let priority_before = book.next_priority;

                let canceled_id = book.orders[cancel_index].original_order().order_id;

                let expected_canceled = before[cancel_index].clone();

                let expected_remaining: Vec<_> = before
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| *index != cancel_index)
                    .map(|(_, state)| state.clone())
                    .collect();

                let expected_fifo: Vec<_> = expected_remaining
                    .iter()
                    .map(|(original, _, _)| original.order_id)
                    .collect();

                // 执行取消。
                let canceled = book.cancel(canceled_id).unwrap();

                // 返回值必须保留完整原始订单、剩余量和优先级。
                assert_eq!(canceled.original_order(), &expected_canceled.0);
                assert_eq!(canceled.remaining(), expected_canceled.1);
                assert_eq!(canceled.priority(), expected_canceled.2);

                // 原始委托数量不能被之前的部分成交修改。
                assert_eq!(canceled.original_order().qty.get(), 10);

                // source Vec 的相对顺序、完整原始订单、
                // remaining 和 QueuePriority 均保持不变。
                assert_eq!(snapshot(&book), expected_remaining);

                // 同价订单仍保持原来的 FIFO 相对顺序。
                assert_eq!(ids(&book.orders(side)), expected_fifo);

                // 取消不分配新的 QueuePriority。
                assert_eq!(book.next_priority, priority_before);

                // 只移除目标订单。
                assert_eq!(book.orders.len(), 2);
                assert!(
                    book.orders
                        .iter()
                        .all(|order| { order.original_order().order_id != canceled_id })
                );
            }
        }
    }
}

#[cfg(test)]
mod maker_query_tests {
    //! 验证 maker 查询的价格交叉、FIFO 与只读性质。
    use super::*;
    use matching_domain::{OrderId, Price, Qty, UserId};

    /// 构造指定方向、价格和数量的订单 fixture。
    fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
        LimitGtcOrder {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            price: Price::try_new(price).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
        }
    }

    /// 将 maker 查询结果投影为订单身份，便于断言选择结果。
    fn maker_id(maker: Option<&ReferenceOrder>) -> Option<OrderId> {
        maker.map(|maker| maker.original_order().order_id)
    }

    /// 构造不交叉的双侧盘口，其中后挂入订单拥有更优价格。
    fn two_sided_book() -> ReferenceOrderBook {
        let mut book = ReferenceOrderBook::new();

        for resting in [
            order(11, Side::Sell, 110, 10),
            order(21, Side::Buy, 90, 10),
            order(12, Side::Sell, 105, 10),
            order(22, Side::Buy, 95, 10),
        ] {
            book.rest(resting).unwrap();
        }

        book
    }

    #[test]
    fn buy_crosses_best_ask_and_sell_crosses_best_bid() {
        let book = two_sided_book();

        assert_eq!(
            maker_id(book.best_crossing_maker(&order(100, Side::Buy, 108, 10))),
            Some(OrderId::new(12))
        );

        assert_eq!(
            maker_id(book.best_crossing_maker(&order(101, Side::Sell, 92, 10))),
            Some(OrderId::new(22))
        );
    }

    #[test]
    fn equal_price_is_crossing_on_both_sides() {
        let book = two_sided_book();

        assert_eq!(
            maker_id(book.best_crossing_maker(&order(100, Side::Buy, 105, 10))),
            Some(OrderId::new(12))
        );

        assert_eq!(
            maker_id(book.best_crossing_maker(&order(101, Side::Sell, 95, 10))),
            Some(OrderId::new(22))
        );
    }

    #[test]
    fn non_crossing_orders_return_none_on_both_sides() {
        let book = two_sided_book();

        // 买入价格低于最优卖价。
        assert!(
            book.best_crossing_maker(&order(100, Side::Buy, 104, 10))
                .is_none()
        );

        // 卖出价格高于最优买价。
        assert!(
            book.best_crossing_maker(&order(101, Side::Sell, 96, 10))
                .is_none()
        );
    }

    #[test]
    fn empty_opposite_side_returns_none() {
        let empty = ReferenceOrderBook::new();

        assert!(
            empty
                .best_crossing_maker(&order(100, Side::Buy, 100, 10))
                .is_none()
        );

        assert!(
            empty
                .best_crossing_maker(&order(101, Side::Sell, 100, 10))
                .is_none()
        );

        let mut buy_only = ReferenceOrderBook::new();
        buy_only.rest(order(1, Side::Buy, 90, 10)).unwrap();

        assert!(
            buy_only
                .best_crossing_maker(&order(100, Side::Buy, 100, 10))
                .is_none()
        );

        let mut sell_only = ReferenceOrderBook::new();
        sell_only.rest(order(2, Side::Sell, 110, 10)).unwrap();

        assert!(
            sell_only
                .best_crossing_maker(&order(101, Side::Sell, 100, 10))
                .is_none()
        );
    }

    #[test]
    fn same_price_makers_preserve_fifo_on_both_sides() {
        let mut book = ReferenceOrderBook::new();

        for resting in [
            order(30, Side::Sell, 105, 10),
            order(130, Side::Buy, 95, 10),
            order(10, Side::Sell, 105, 10),
            order(110, Side::Buy, 95, 10),
            order(20, Side::Sell, 105, 10),
            order(120, Side::Buy, 95, 10),
        ] {
            book.rest(resting).unwrap();
        }

        // ID 的数值顺序故意不同于挂单顺序。
        assert_eq!(
            maker_id(book.best_crossing_maker(&order(200, Side::Buy, 105, 10))),
            Some(OrderId::new(30))
        );

        assert_eq!(
            maker_id(book.best_crossing_maker(&order(201, Side::Sell, 95, 10))),
            Some(OrderId::new(130))
        );
    }

    #[test]
    fn exhausted_maker_does_not_block_next_maker() {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(30, Side::Sell, 105, 1)).unwrap();
        book.rest(order(10, Side::Sell, 105, 1)).unwrap();

        // 测试模块内部模拟已经完整成交的 resting 订单。
        book.orders[0].apply_fill(Qty::try_new(1).unwrap()).unwrap();

        assert_eq!(book.orders[0].remaining(), 0);

        assert_eq!(
            maker_id(book.best_crossing_maker(&order(100, Side::Buy, 105, 1))),
            Some(OrderId::new(10))
        );
    }

    #[test]
    fn repeated_queries_preserve_all_book_state() {
        let mut book = ReferenceOrderBook::new();

        for resting in [
            order(30, Side::Sell, 105, 10),
            order(20, Side::Buy, 95, 10),
            order(10, Side::Sell, 105, 10),
        ] {
            book.rest(resting).unwrap();
        }

        book.orders[0].apply_fill(Qty::try_new(3).unwrap()).unwrap();

        // 包括源顺序、全部原始字段、remaining 和 priority。
        let snapshot = |book: &ReferenceOrderBook| {
            book.orders
                .iter()
                .map(|resting| {
                    (
                        resting.original_order().clone(),
                        resting.remaining(),
                        resting.priority(),
                    )
                })
                .collect::<Vec<_>>()
        };

        let before = snapshot(&book);
        let next_before = book.next_priority;

        let incoming_buy = order(100, Side::Buy, 105, 10);
        let incoming_sell = order(101, Side::Sell, 95, 10);

        for _ in 0..5 {
            assert_eq!(
                maker_id(book.best_crossing_maker(&incoming_buy)),
                Some(OrderId::new(30))
            );

            assert_eq!(
                maker_id(book.best_crossing_maker(&incoming_sell)),
                Some(OrderId::new(20))
            );
        }

        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, next_before);
        assert_eq!(book.orders.len(), 3);
        assert_eq!(book.orders[0].remaining(), 7);
        assert_eq!(book.orders[0].original_order().qty.get(), 10);
    }

    fn assert_index_matches_reference(
        book: &ReferenceOrderBook,
        incoming: &LimitGtcOrder,
        expected: Option<usize>,
    ) {
        let index = book.best_crossing_maker_index(incoming);
        let reference = book.best_crossing_maker(incoming);

        assert_eq!(index, expected);

        match (index, reference) {
            (Some(index), Some(maker)) => {
                assert!(std::ptr::eq(maker, &book.orders[index]));
            }
            (None, None) => {}
            _ => panic!("maker index and public reference disagree"),
        }
    }

    #[test]
    fn index_skips_exhausted_makers_on_both_sides() {
        let mut book = ReferenceOrderBook::new();

        for resting in [
            order(30, Side::Sell, 105, 1),
            order(130, Side::Buy, 95, 1),
            order(10, Side::Sell, 105, 1),
            order(110, Side::Buy, 95, 1),
        ] {
            book.rest(resting).unwrap();
        }

        // 仅测试 fixture 修改运行态，查询自身不负责扣减。
        book.orders[0].apply_fill(Qty::try_new(1).unwrap()).unwrap();

        book.orders[1].apply_fill(Qty::try_new(1).unwrap()).unwrap();

        assert_eq!(book.orders[0].remaining(), 0);
        assert_eq!(book.orders[1].remaining(), 0);

        // 跳过耗尽订单，选择下一张同价 maker。
        assert_index_matches_reference(&book, &order(200, Side::Buy, 105, 1), Some(2));

        assert_index_matches_reference(&book, &order(201, Side::Sell, 95, 1), Some(3));
    }
}

#[cfg(test)]
mod next_fill_planner_tests {
    //! 验证只读 next-fill 规划的 maker、数量和状态保持性质。
    use super::ReferenceOrderBook;
    use crate::IncomingOrder;
    use matching_domain::{LimitGtcOrder, OrderId, Price, Qty, QueuePriority, Side, UserId};

    /// 构造指定方向、价格和数量的订单 fixture。
    fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
        LimitGtcOrder {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            price: Price::try_new(price).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
        }
    }

    /// 将指定订单包装为尚未成交的 incoming 运行态。
    fn incoming(id: u128, side: Side, price: i64, qty: u64) -> IncomingOrder {
        IncomingOrder::new(order(id, side, price, qty))
    }

    /// 构造最高 bid 为 95、最低 ask 为 105 的不交叉盘口。
    fn two_sided_book() -> ReferenceOrderBook {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, Side::Sell, 105, 10)).unwrap();
        book.rest(order(2, Side::Buy, 95, 10)).unwrap();

        book
    }

    /// 验证公开规划结果、maker 对象身份与成交数量。
    fn assert_plan(
        book: &ReferenceOrderBook,
        incoming: &IncomingOrder,
        expected: Option<(usize, u64)>,
    ) {
        let actual = book.next_fill_plan(incoming).unwrap();

        match (actual, expected) {
            (None, None) => {
                assert!(
                    book.best_crossing_maker(incoming.original_order())
                        .is_none()
                );
            }

            (Some((maker, qty)), Some((index, lots))) => {
                // 必须是源 Vec 中的同一个 ReferenceOrder。
                assert!(std::ptr::eq(maker, &book.orders[index]));

                // 必须与已有的 maker 选择逻辑保持一致。
                let selected = book.best_crossing_maker(incoming.original_order()).unwrap();

                assert!(std::ptr::eq(maker, selected));

                // 计划成交量始终是合法的正 Qty。
                assert_eq!(qty.get(), lots);
                assert!(qty.get() > 0);
            }

            (actual, expected) => {
                panic!(
                    "unexpected plan: actual={actual:?}, \
                     expected={expected:?}"
                );
            }
        }
    }

    /// 保存完整订单、运行态剩余量和 FIFO priority。
    fn snapshot(book: &ReferenceOrderBook) -> Vec<(LimitGtcOrder, u64, QueuePriority)> {
        book.orders
            .iter()
            .map(|resting| {
                (
                    resting.original_order().clone(),
                    resting.remaining(),
                    resting.priority(),
                )
            })
            .collect()
    }

    #[test]
    fn buy_and_sell_exact_fill() {
        let book = two_sided_book();

        // Buy @105 = Ask @105。
        assert_plan(&book, &incoming(100, Side::Buy, 105, 10), Some((0, 10)));

        // Sell @95 = Bid @95。
        assert_plan(&book, &incoming(101, Side::Sell, 95, 10), Some((1, 10)));

        // 查询不会执行成交。
        assert_eq!(book.orders[0].remaining(), 10);
        assert_eq!(book.orders[1].remaining(), 10);
    }

    #[test]
    fn buy_and_sell_cross_at_better_prices() {
        let book = two_sided_book();

        assert_plan(&book, &incoming(100, Side::Buy, 110, 10), Some((0, 10)));

        assert_plan(&book, &incoming(101, Side::Sell, 90, 10), Some((1, 10)));
    }

    #[test]
    fn maker_partial_fill_on_both_sides() {
        let book = two_sided_book();

        // incoming 比 maker 小，maker 只会部分成交。
        assert_plan(&book, &incoming(100, Side::Buy, 105, 4), Some((0, 4)));

        assert_plan(&book, &incoming(101, Side::Sell, 95, 6), Some((1, 6)));

        // 这里只生成计划，没有实际扣减。
        assert_eq!(book.orders[0].remaining(), 10);
        assert_eq!(book.orders[1].remaining(), 10);
    }

    #[test]
    fn taker_partial_fill_on_both_sides() {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, Side::Sell, 105, 3)).unwrap();

        book.rest(order(2, Side::Buy, 95, 4)).unwrap();

        // incoming 比 maker 大，只能成交 maker 的剩余量。
        assert_plan(&book, &incoming(100, Side::Buy, 105, 10), Some((0, 3)));

        assert_plan(&book, &incoming(101, Side::Sell, 95, 10), Some((1, 4)));

        assert_eq!(book.orders[0].remaining(), 3);
        assert_eq!(book.orders[1].remaining(), 4);
    }

    #[test]
    fn empty_book_returns_no_plan() {
        let book = ReferenceOrderBook::new();

        assert_plan(&book, &incoming(100, Side::Buy, 105, 10), None);

        assert_plan(&book, &incoming(101, Side::Sell, 95, 10), None);
    }

    #[test]
    fn non_crossing_prices_return_no_plan() {
        let book = two_sided_book();

        // Buy @104 < Ask @105。
        assert_plan(&book, &incoming(100, Side::Buy, 104, 10), None);

        // Sell @96 > Bid @95。
        assert_plan(&book, &incoming(101, Side::Sell, 96, 10), None);
    }

    #[test]
    fn empty_opposite_side_returns_no_plan() {
        let mut buy_only = ReferenceOrderBook::new();

        buy_only.rest(order(1, Side::Buy, 95, 10)).unwrap();

        // incoming 也是 Buy，没有卖盘。
        assert_plan(&buy_only, &incoming(100, Side::Buy, 200, 10), None);

        let mut sell_only = ReferenceOrderBook::new();

        sell_only.rest(order(2, Side::Sell, 105, 10)).unwrap();

        // incoming 也是 Sell，没有买盘。
        assert_plan(&sell_only, &incoming(101, Side::Sell, 1, 10), None);
    }

    #[test]
    fn best_price_takes_precedence_over_earlier_worse_price() {
        let mut book = ReferenceOrderBook::new();

        // 更优价格的 maker 后挂入。
        for resting in [
            order(11, Side::Sell, 110, 10),
            order(21, Side::Buy, 90, 10),
            order(12, Side::Sell, 105, 10),
            order(22, Side::Buy, 95, 10),
        ] {
            book.rest(resting).unwrap();
        }

        assert_plan(&book, &incoming(100, Side::Buy, 110, 10), Some((2, 10)));

        assert_plan(&book, &incoming(101, Side::Sell, 90, 10), Some((3, 10)));
    }

    #[test]
    fn same_price_preserves_fifo_on_both_sides() {
        let mut book = ReferenceOrderBook::new();

        // 同价 ID 顺序故意与数值顺序不同。
        for resting in [
            order(30, Side::Sell, 105, 3),
            order(130, Side::Buy, 95, 3),
            order(10, Side::Sell, 105, 7),
            order(110, Side::Buy, 95, 8),
            order(20, Side::Sell, 105, 9),
            order(120, Side::Buy, 95, 10),
        ] {
            book.rest(resting).unwrap();
        }

        let buy = incoming(200, Side::Buy, 105, 10);
        let sell = incoming(201, Side::Sell, 95, 10);

        // 最早挂入的同价 maker 获得优先级。
        assert_plan(&book, &buy, Some((0, 3)));
        assert_plan(&book, &sell, Some((1, 3)));

        assert_eq!(
            book.best_crossing_maker(buy.original_order())
                .unwrap()
                .original_order()
                .order_id,
            OrderId::new(30),
        );

        assert_eq!(
            book.best_crossing_maker(sell.original_order())
                .unwrap()
                .original_order()
                .order_id,
            OrderId::new(130),
        );
    }

    #[test]
    fn exhausted_makers_are_skipped_and_fifo_is_preserved() {
        let mut book = ReferenceOrderBook::new();

        for resting in [
            order(30, Side::Sell, 105, 3),
            order(130, Side::Buy, 95, 3),
            order(10, Side::Sell, 105, 7),
            order(110, Side::Buy, 95, 8),
        ] {
            book.rest(resting).unwrap();
        }

        let buy = incoming(200, Side::Buy, 105, 10);
        let sell = incoming(201, Side::Sell, 95, 10);

        // 耗尽之前，先选择最早挂入的订单。
        assert_plan(&book, &buy, Some((0, 3)));
        assert_plan(&book, &sell, Some((1, 3)));

        // 模拟已有成交，耗尽最早的两张 maker。
        book.orders[0].apply_fill(Qty::try_new(3).unwrap()).unwrap();

        book.orders[1].apply_fill(Qty::try_new(3).unwrap()).unwrap();

        assert_eq!(book.orders[0].remaining(), 0);
        assert_eq!(book.orders[1].remaining(), 0);

        // 跳过耗尽订单，选择下一张同价 maker。
        assert_plan(&book, &buy, Some((2, 7)));
        assert_plan(&book, &sell, Some((3, 8)));
    }

    #[test]
    fn all_exhausted_makers_produce_no_plan() {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, Side::Sell, 105, 3)).unwrap();

        book.rest(order(2, Side::Buy, 95, 4)).unwrap();

        book.orders[0].apply_fill(Qty::try_new(3).unwrap()).unwrap();

        book.orders[1].apply_fill(Qty::try_new(4).unwrap()).unwrap();

        assert_plan(&book, &incoming(100, Side::Buy, 105, 10), None);

        assert_plan(&book, &incoming(101, Side::Sell, 95, 10), None);
    }

    #[test]
    fn maximum_quantity_is_preserved_in_plan() {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, Side::Sell, 105, u64::MAX)).unwrap();

        assert_plan(
            &book,
            &incoming(100, Side::Buy, 105, u64::MAX),
            Some((0, u64::MAX)),
        );

        assert_eq!(book.orders[0].remaining(), u64::MAX);
    }

    #[test]
    fn repeated_planning_does_not_mutate_book_or_incoming() {
        let mut book = ReferenceOrderBook::new();

        for resting in [
            order(30, Side::Sell, 105, 10),
            order(20, Side::Buy, 95, 8),
            order(10, Side::Sell, 105, 12),
        ] {
            book.rest(resting).unwrap();
        }

        // 预先制造部分成交的 resting maker。
        book.orders[0].apply_fill(Qty::try_new(3).unwrap()).unwrap();

        let buy = incoming(100, Side::Buy, 105, 9);
        let sell = incoming(101, Side::Sell, 95, 3);

        let book_before = snapshot(&book);
        let next_priority_before = book.next_priority;

        let buy_original = buy.original_order().clone();
        let sell_original = sell.original_order().clone();

        let buy_remaining = buy.remaining();
        let sell_remaining = sell.remaining();

        for _ in 0..10 {
            assert_plan(&book, &buy, Some((0, 7)));
            assert_plan(&book, &sell, Some((1, 3)));
        }

        // 订单簿的全部运行态保持不变。
        assert_eq!(snapshot(&book), book_before);
        assert_eq!(book.next_priority, next_priority_before);
        assert_eq!(book.orders.len(), 3);

        // incoming 的原始订单和运行态也保持不变。
        assert_eq!(buy.original_order(), &buy_original);
        assert_eq!(sell.original_order(), &sell_original);

        assert_eq!(buy.remaining(), buy_remaining);
        assert_eq!(sell.remaining(), sell_remaining);

        // 原始订单数量不受规划影响。
        assert_eq!(book.orders[0].remaining(), 7);
        assert_eq!(book.orders[0].original_order().qty.get(), 10);
    }
    #[test]
    fn partially_filled_buy_uses_current_remaining() {
        let mut book = two_sided_book();

        // Sell maker: original=10, remaining=8。
        book.orders[0].apply_fill(Qty::try_new(2).unwrap()).unwrap();

        // Buy incoming: original=10, remaining=3。
        let mut buy = incoming(100, Side::Buy, 105, 10);
        buy.apply_fill(Qty::try_new(7).unwrap()).unwrap();

        let book_before = snapshot(&book);
        let priority_before = book.next_priority;
        let original_before = buy.original_order().clone();
        let remaining_before = buy.remaining();

        // min(current remaining=3, maker remaining=8) = 3。
        for _ in 0..3 {
            assert_plan(&book, &buy, Some((0, 3)));
        }

        assert_eq!(snapshot(&book), book_before);
        assert_eq!(book.next_priority, priority_before);
        assert_eq!(book.orders[0].remaining(), 8);
        assert_eq!(book.orders[0].priority(), book_before[0].2);

        assert_eq!(buy.original_order(), &original_before);
        assert_eq!(buy.original_order().qty.get(), 10);
        assert_eq!(buy.remaining(), remaining_before);
        assert_eq!(buy.remaining(), 3);
    }

    #[test]
    fn partially_filled_sell_uses_current_remaining() {
        let mut book = two_sided_book();

        // Buy maker: original=10, remaining=4。
        book.orders[1].apply_fill(Qty::try_new(6).unwrap()).unwrap();

        // Sell incoming: original=10, remaining=7。
        let mut sell = incoming(101, Side::Sell, 95, 10);
        sell.apply_fill(Qty::try_new(3).unwrap()).unwrap();

        let book_before = snapshot(&book);
        let priority_before = book.next_priority;
        let original_before = sell.original_order().clone();
        let remaining_before = sell.remaining();

        // min(current remaining=7, maker remaining=4) = 4。
        for _ in 0..3 {
            assert_plan(&book, &sell, Some((1, 4)));
        }

        assert_eq!(snapshot(&book), book_before);
        assert_eq!(book.next_priority, priority_before);
        assert_eq!(book.orders[1].remaining(), 4);
        assert_eq!(book.orders[1].priority(), book_before[1].2);

        assert_eq!(sell.original_order(), &original_before);
        assert_eq!(sell.original_order().qty.get(), 10);
        assert_eq!(sell.remaining(), remaining_before);
        assert_eq!(sell.remaining(), 7);
    }

    #[test]
    fn exhausted_buy_returns_none_with_crossing_maker() {
        let book = two_sided_book();

        let mut buy = incoming(100, Side::Buy, 105, 10);
        buy.apply_fill(Qty::try_new(10).unwrap()).unwrap();

        let book_before = snapshot(&book);
        let priority_before = book.next_priority;
        let original_before = buy.original_order().clone();
        let remaining_before = buy.remaining();

        assert_eq!(buy.remaining(), 0);

        // 确认存在 crossing maker，避免测试因无对手盘而通过。
        assert!(book.best_crossing_maker(buy.original_order()).is_some());

        for _ in 0..3 {
            assert!(matches!(book.next_fill_plan(&buy), Ok(None)));
        }

        assert_eq!(snapshot(&book), book_before);
        assert_eq!(book.next_priority, priority_before);
        assert_eq!(book.orders[0].remaining(), 10);
        assert_eq!(book.orders[0].priority(), book_before[0].2);

        assert_eq!(buy.original_order(), &original_before);
        assert_eq!(buy.original_order().qty.get(), 10);
        assert_eq!(buy.remaining(), remaining_before);
        assert_eq!(buy.remaining(), 0);
    }

    #[test]
    fn exhausted_sell_returns_none_with_crossing_maker() {
        let book = two_sided_book();

        let mut sell = incoming(101, Side::Sell, 95, 10);
        sell.apply_fill(Qty::try_new(10).unwrap()).unwrap();

        let book_before = snapshot(&book);
        let priority_before = book.next_priority;
        let original_before = sell.original_order().clone();
        let remaining_before = sell.remaining();

        assert_eq!(sell.remaining(), 0);

        // 确认存在 crossing maker。
        assert!(book.best_crossing_maker(sell.original_order()).is_some());

        for _ in 0..3 {
            assert!(matches!(book.next_fill_plan(&sell), Ok(None)));
        }

        assert_eq!(snapshot(&book), book_before);
        assert_eq!(book.next_priority, priority_before);
        assert_eq!(book.orders[1].remaining(), 10);
        assert_eq!(book.orders[1].priority(), book_before[1].2);

        assert_eq!(sell.original_order(), &original_before);
        assert_eq!(sell.original_order().qty.get(), 10);
        assert_eq!(sell.remaining(), remaining_before);
        assert_eq!(sell.remaining(), 0);
    }
}

#[cfg(test)]
mod next_fill_execution_tests {
    //! 验证 next-fill 执行、连续成交、TradeEvent 与固定命令带确定性。
    use super::*;
    use matching_domain::{OrderId, Price, Qty, UserId};

    /// 构造指定方向、价格和数量的订单 fixture。
    fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
        LimitGtcOrder {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            price: Price::try_new(price).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
        }
    }

    /// 将指定订单包装为尚未成交的 incoming 运行态。
    fn incoming(id: u128, side: Side, price: i64, qty: u64) -> IncomingOrder {
        IncomingOrder::new(order(id, side, price, qty))
    }

    /// 保存源 `Vec` 顺序、原始订单、remaining 和 priority。
    fn snapshot(book: &ReferenceOrderBook) -> Vec<(LimitGtcOrder, u64, QueuePriority)> {
        book.orders
            .iter()
            .map(|maker| {
                (
                    maker.original_order().clone(),
                    maker.remaining(),
                    maker.priority(),
                )
            })
            .collect()
    }

    #[test]
    fn cross_price_uses_maker_identity_and_price() {
        for (side, maker_side, maker_price, taker_price) in [
            (Side::Buy, Side::Sell, 105, 110),
            (Side::Sell, Side::Buy, 95, 90),
        ] {
            let mut book = ReferenceOrderBook::new();

            book.rest(order(1, maker_side, maker_price, 10)).unwrap();

            let mut taker = incoming(2, side, taker_price, 3);
            let original = taker.original_order().clone();
            let priority = book.orders[0].priority();

            let result = book.execute_next_fill(&mut taker);

            assert_eq!(
                result,
                Ok(Some((
                    OrderId::new(1),
                    Price::try_new(maker_price).unwrap(),
                    Qty::try_new(3).unwrap(),
                )))
            );

            assert_eq!(book.orders.len(), 1);
            assert_eq!(book.orders[0].remaining(), 7);
            assert_eq!(book.orders[0].priority(), priority);
            assert_eq!(taker.remaining(), 0);
            assert_eq!(taker.original_order(), &original);
        }
    }

    #[test]
    fn exact_fill_removes_maker_and_exhausts_incoming() {
        for (side, maker_side, maker_price, taker_price) in [
            (Side::Buy, Side::Sell, 105, 110),
            (Side::Sell, Side::Buy, 95, 90),
        ] {
            let mut book = ReferenceOrderBook::new();

            book.rest(order(1, maker_side, maker_price, 10)).unwrap();

            let mut taker = incoming(2, side, taker_price, 10);
            let original = taker.original_order().clone();
            let next_priority = book.next_priority;

            assert_eq!(
                book.execute_next_fill(&mut taker),
                Ok(Some((
                    OrderId::new(1),
                    Price::try_new(maker_price).unwrap(),
                    Qty::try_new(10).unwrap(),
                )))
            );

            assert!(book.orders.is_empty());
            assert_eq!(book.next_priority, next_priority);

            assert_eq!(taker.remaining(), 0);
            assert_eq!(taker.original_order(), &original);
        }
    }

    #[test]
    fn partial_maker_keeps_priority() {
        for (side, maker_side, maker_price, taker_price) in [
            (Side::Buy, Side::Sell, 105, 110),
            (Side::Sell, Side::Buy, 95, 90),
        ] {
            let mut book = ReferenceOrderBook::new();

            book.rest(order(1, maker_side, maker_price, 10)).unwrap();

            let priority = book.orders[0].priority();
            let next_priority = book.next_priority;

            let mut taker = incoming(2, side, taker_price, 6);

            assert!(matches!(
                book.execute_next_fill(&mut taker),
                Ok(Some((_, _, qty))) if qty.get() == 6
            ));

            assert_eq!(book.orders.len(), 1);
            assert_eq!(book.orders[0].remaining(), 4);
            assert_eq!(book.orders[0].priority(), priority);
            assert_eq!(book.next_priority, next_priority);
            assert_eq!(taker.remaining(), 0);
        }
    }

    #[test]
    fn filled_maker_preserves_incoming_remainder() {
        for (side, maker_side, maker_price, taker_price) in [
            (Side::Buy, Side::Sell, 105, 110),
            (Side::Sell, Side::Buy, 95, 90),
        ] {
            let mut book = ReferenceOrderBook::new();

            book.rest(order(1, maker_side, maker_price, 4)).unwrap();

            let mut taker = incoming(2, side, taker_price, 10);
            let original = taker.original_order().clone();

            assert!(matches!(
                book.execute_next_fill(&mut taker),
                Ok(Some((_, _, qty))) if qty.get() == 4
            ));

            assert!(book.orders.is_empty());

            // 剩余量保留在 incoming，不自动 rest。
            assert_eq!(taker.remaining(), 6);
            assert_eq!(taker.original_order(), &original);
        }
    }

    #[test]
    fn execution_uses_current_incoming_remaining() {
        for (side, maker_side, maker_price, taker_price) in [
            (Side::Buy, Side::Sell, 105, 110),
            (Side::Sell, Side::Buy, 95, 90),
        ] {
            let mut book = ReferenceOrderBook::new();

            book.rest(order(1, maker_side, maker_price, 8)).unwrap();

            let mut taker = incoming(2, side, taker_price, 10);

            taker.apply_fill(Qty::try_new(7).unwrap()).unwrap();

            let original = taker.original_order().clone();
            let priority = book.orders[0].priority();

            assert_eq!(
                book.execute_next_fill(&mut taker),
                Ok(Some((
                    OrderId::new(1),
                    Price::try_new(maker_price).unwrap(),
                    Qty::try_new(3).unwrap(),
                )))
            );

            assert_eq!(book.orders[0].remaining(), 5);
            assert_eq!(book.orders[0].priority(), priority);
            assert_eq!(taker.remaining(), 0);
            assert_eq!(taker.original_order(), &original);
        }
    }

    #[test]
    fn no_plan_preserves_all_state() {
        for (side, incoming_price) in [(Side::Buy, 104), (Side::Sell, 96)] {
            let mut book = ReferenceOrderBook::new();

            book.rest(order(1, Side::Sell, 105, 10)).unwrap();

            book.rest(order(2, Side::Buy, 95, 10)).unwrap();

            let mut taker = incoming(3, side, incoming_price, 10);

            let before = snapshot(&book);
            let priority_before = book.next_priority;
            let original = taker.original_order().clone();
            let remaining = taker.remaining();

            assert!(matches!(book.execute_next_fill(&mut taker), Ok(None)));

            assert_eq!(snapshot(&book), before);
            assert_eq!(book.next_priority, priority_before);
            assert_eq!(taker.original_order(), &original);
            assert_eq!(taker.remaining(), remaining);
        }
    }

    #[test]
    fn exhausted_incoming_returns_none() {
        for (side, maker_side, maker_price, taker_price) in [
            (Side::Buy, Side::Sell, 105, 110),
            (Side::Sell, Side::Buy, 95, 90),
        ] {
            let mut book = ReferenceOrderBook::new();

            book.rest(order(1, maker_side, maker_price, 10)).unwrap();

            let mut taker = incoming(2, side, taker_price, 10);

            taker.apply_fill(Qty::try_new(10).unwrap()).unwrap();

            assert!(book.best_crossing_maker(taker.original_order()).is_some());

            let before = snapshot(&book);
            let priority_before = book.next_priority;
            let original = taker.original_order().clone();

            assert!(matches!(book.execute_next_fill(&mut taker), Ok(None)));

            assert_eq!(snapshot(&book), before);
            assert_eq!(book.next_priority, priority_before);
            assert_eq!(taker.original_order(), &original);
            assert_eq!(taker.remaining(), 0);
        }
    }
    #[test]
    fn multi_fill_respects_price_then_fifo_and_maker_prices_on_both_sides() {
        for (side, maker_side, taker_price, better, worse) in [
            (Side::Buy, Side::Sell, 120, 105, 110),
            (Side::Sell, Side::Buy, 80, 95, 90),
        ] {
            let mut book = ReferenceOrderBook::new();
            // 故意让更差价格先挂入；同价订单的 ID 与 FIFO 顺序相反。
            book.rest(order(90, maker_side, worse, 3)).unwrap();
            book.rest(order(30, maker_side, better, 2)).unwrap();
            book.rest(order(10, maker_side, better, 4)).unwrap();
            let next_before = book.next_priority;
            let mut taker = incoming(200, side, taker_price, 8);
            let original = taker.original_order().clone();

            let fills = book.execute_fills(&mut taker).unwrap();
            assert_eq!(
                fills,
                vec![
                    (
                        OrderId::new(30),
                        Price::try_new(better).unwrap(),
                        Qty::try_new(2).unwrap()
                    ),
                    (
                        OrderId::new(10),
                        Price::try_new(better).unwrap(),
                        Qty::try_new(4).unwrap()
                    ),
                    (
                        OrderId::new(90),
                        Price::try_new(worse).unwrap(),
                        Qty::try_new(2).unwrap()
                    ),
                ]
            );
            assert_eq!(taker.remaining(), 0);
            assert_eq!(taker.original_order(), &original);
            assert_eq!(book.orders.len(), 1);
            assert_eq!(book.orders[0].original_order().order_id, OrderId::new(90));
            assert_eq!(book.orders[0].remaining(), 1);
            assert_eq!(book.orders[0].priority(), QueuePriority::new(0));
            assert_eq!(book.next_priority, next_before);
        }
    }

    #[test]
    fn multi_fill_keeps_unmatched_incoming_and_does_not_rest_it() {
        for (side, maker_side, taker_price, maker_price) in [
            (Side::Buy, Side::Sell, 110, 105),
            (Side::Sell, Side::Buy, 90, 95),
        ] {
            let mut book = ReferenceOrderBook::new();
            book.rest(order(1, maker_side, maker_price, 2)).unwrap();
            book.rest(order(2, maker_side, maker_price, 3)).unwrap();
            let next_before = book.next_priority;
            let mut taker = incoming(100, side, taker_price, 9);
            let original = taker.original_order().clone();

            assert_eq!(
                book.execute_fills(&mut taker).unwrap(),
                vec![
                    (
                        OrderId::new(1),
                        Price::try_new(maker_price).unwrap(),
                        Qty::try_new(2).unwrap()
                    ),
                    (
                        OrderId::new(2),
                        Price::try_new(maker_price).unwrap(),
                        Qty::try_new(3).unwrap()
                    ),
                ]
            );
            assert!(book.orders.is_empty());
            assert_eq!(book.next_priority, next_before);
            assert_eq!(taker.remaining(), 4);
            assert_eq!(taker.original_order(), &original);
        }
    }

    #[test]
    fn multi_fill_stops_at_non_crossing_price_and_preserves_resting_order() {
        for (side, maker_side, taker_price, crossing, non_crossing) in [
            (Side::Buy, Side::Sell, 107, 105, 110),
            (Side::Sell, Side::Buy, 93, 95, 90),
        ] {
            let mut book = ReferenceOrderBook::new();
            book.rest(order(1, maker_side, non_crossing, 6)).unwrap();
            book.rest(order(2, maker_side, crossing, 3)).unwrap();
            let kept_original = book.orders[0].original_order().clone();
            let kept_priority = book.orders[0].priority();
            let next_before = book.next_priority;
            let mut taker = incoming(100, side, taker_price, 8);
            let original = taker.original_order().clone();

            assert_eq!(
                book.execute_fills(&mut taker).unwrap(),
                vec![(
                    OrderId::new(2),
                    Price::try_new(crossing).unwrap(),
                    Qty::try_new(3).unwrap()
                )]
            );
            assert_eq!(taker.remaining(), 5);
            assert_eq!(taker.original_order(), &original);
            assert_eq!(book.orders.len(), 1);
            assert_eq!(book.orders[0].original_order(), &kept_original);
            assert_eq!(book.orders[0].remaining(), 6);
            assert_eq!(book.orders[0].priority(), kept_priority);
            assert_eq!(book.next_priority, next_before);
        }
    }

    #[test]
    fn multi_fill_with_no_plan_or_exhausted_incoming_is_read_only() {
        for (side, taker_price) in [(Side::Buy, 104), (Side::Sell, 96)] {
            let mut book = ReferenceOrderBook::new();
            book.rest(order(1, Side::Sell, 105, 10)).unwrap();
            book.rest(order(2, Side::Buy, 95, 10)).unwrap();
            let before = snapshot(&book);
            let next_before = book.next_priority;
            let mut taker = incoming(100, side, taker_price, 10);
            let original = taker.original_order().clone();

            assert!(book.execute_fills(&mut taker).unwrap().is_empty());
            assert_eq!(snapshot(&book), before);
            assert_eq!(book.next_priority, next_before);
            assert_eq!(taker.remaining(), 10);
            assert_eq!(taker.original_order(), &original);
        }

        let mut book = ReferenceOrderBook::new();
        book.rest(order(1, Side::Sell, 105, 10)).unwrap();
        let before = snapshot(&book);
        let next_before = book.next_priority;
        let mut taker = incoming(100, Side::Buy, 110, 10);
        taker.apply_fill(Qty::try_new(10).unwrap()).unwrap();
        let original = taker.original_order().clone();

        assert!(book.execute_fills(&mut taker).unwrap().is_empty());
        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, next_before);
        assert_eq!(taker.remaining(), 0);
        assert_eq!(taker.original_order(), &original);
    }

    #[test]
    fn empty_book_execute_fills_preserves_all_state() {
        let mut book = ReferenceOrderBook::new();
        let mut taker = incoming(100, Side::Buy, 105, 10);

        let book_before = snapshot(&book);
        let priority_before = book.next_priority;
        let original_before = taker.original_order().clone();
        let remaining_before = taker.remaining();

        let fills = book.execute_fills(&mut taker).unwrap();

        // 空簿不产生任何成交。
        assert!(fills.is_empty());

        // 订单簿及优先级计数器保持不变。
        assert_eq!(snapshot(&book), book_before);
        assert!(book.orders.is_empty());
        assert_eq!(book.next_priority, priority_before);

        // IncomingOrder 的原始订单及剩余量保持不变。
        assert_eq!(taker.original_order(), &original_before);
        assert_eq!(taker.remaining(), remaining_before);
    }
    #[test]
    fn trade_event_uses_supplied_id_and_maker_price() {
        for (side, maker_side, maker_price, taker_price) in [
            (Side::Buy, Side::Sell, 105, 110),
            (Side::Sell, Side::Buy, 95, 90),
        ] {
            let mut book = ReferenceOrderBook::new();
            book.rest(order(1, maker_side, maker_price, 10)).unwrap();

            let mut taker = incoming(2, side, taker_price, 3);
            let trade_id = TradeId::new(100);

            let trade = book
                .execute_next_fill_trade(&mut taker, trade_id)
                .unwrap()
                .unwrap();

            assert_eq!(trade.trade_id, trade_id);
            assert_eq!(trade.maker_order_id, OrderId::new(1));
            assert_eq!(trade.taker_order_id, OrderId::new(2));
            assert_eq!(trade.price, Price::try_new(maker_price).unwrap());
            assert_eq!(trade.qty.get(), 3);
            assert_eq!(trade.maker_side, maker_side);

            assert_eq!(book.orders[0].remaining(), 7);
            assert_eq!(taker.remaining(), 0);
        }
    }
    #[test]
    fn trade_event_preserves_execution_semantics() {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, Side::Sell, 105, 4)).unwrap();

        let mut taker = incoming(2, Side::Buy, 110, 10);

        let trade = book
            .execute_next_fill_trade(&mut taker, TradeId::new(101))
            .unwrap()
            .unwrap();

        assert_eq!(trade.qty.get(), 4);
        assert_eq!(trade.price.get(), 105);
        assert_eq!(trade.maker_side, Side::Sell);

        assert!(book.orders.is_empty());
        assert_eq!(taker.remaining(), 6);
        assert_eq!(taker.original_order().qty.get(), 10);
    }
    #[test]
    fn no_fill_produces_no_trade_event() {
        let mut book = ReferenceOrderBook::new();

        book.rest(order(1, Side::Sell, 105, 10)).unwrap();

        let mut taker = incoming(2, Side::Buy, 100, 10);

        let before = snapshot(&book);
        let priority_before = book.next_priority;
        let original_before = taker.original_order().clone();
        let remaining_before = taker.remaining();

        let result = book
            .execute_next_fill_trade(&mut taker, TradeId::new(102))
            .unwrap();

        assert!(result.is_none());
        assert_eq!(snapshot(&book), before);
        assert_eq!(book.next_priority, priority_before);
        assert_eq!(taker.original_order(), &original_before);
        assert_eq!(taker.remaining(), remaining_before);
    }
    /// 在独立空簿上执行固定命令带，供确定性回归比较。
    fn run_fixed_command_tape() -> (
        Vec<TradeEvent>,
        Vec<(LimitGtcOrder, u64, QueuePriority)>,
        u128,
    ) {
        // 每次独立从空簿开始。
        let mut book = ReferenceOrderBook::new();
        let mut trades = Vec::new();

        // 固定挂单顺序，priority 分别为 0、1、2、3。
        book.rest(order(30, Side::Sell, 105, 3)).unwrap();
        book.rest(order(10, Side::Sell, 105, 7)).unwrap();
        book.rest(order(20, Side::Buy, 95, 5)).unwrap();
        book.rest(order(40, Side::Sell, 110, 6)).unwrap();

        // Buy @105，初始数量 8。
        let mut taker = incoming(100, Side::Buy, 105, 8);

        // 第一笔：maker 30 完全成交 3。
        trades.push(
            book.execute_next_fill_trade(&mut taker, TradeId::new(501))
                .unwrap()
                .expect("first trade"),
        );

        // 第二笔：maker 10 成交 5，剩余 2。
        trades.push(
            book.execute_next_fill_trade(&mut taker, TradeId::new(502))
                .unwrap()
                .expect("second trade"),
        );

        // Incoming 已耗尽，不应继续成交。
        assert_eq!(taker.remaining(), 0);
        assert_eq!(taker.original_order().qty.get(), 8);

        // 取消部分成交后的 maker 10。
        let canceled = book.cancel(OrderId::new(10)).unwrap();

        assert_eq!(canceled.original_order().order_id, OrderId::new(10));
        assert_eq!(canceled.original_order().qty.get(), 7);
        assert_eq!(canceled.remaining(), 2);
        assert_eq!(canceled.priority(), QueuePriority::new(1));

        let final_snapshot = snapshot(&book);
        let next_priority = book.next_priority;

        (trades, final_snapshot, next_priority)
    }
    #[test]
    fn fixed_command_tape_is_deterministic() {
        let first = run_fixed_command_tape();
        let second = run_fixed_command_tape();

        // 两次完全独立运行，结果必须相同。
        assert_eq!(first, second);

        let (trades, final_book, next_priority) = first;

        // 手工定义预期事件顺序。
        let expected_trades = vec![
            TradeEvent {
                trade_id: TradeId::new(501),
                maker_order_id: OrderId::new(30),
                taker_order_id: OrderId::new(100),
                price: Price::try_new(105).unwrap(),
                qty: Qty::try_new(3).unwrap(),
                maker_side: Side::Sell,
            },
            TradeEvent {
                trade_id: TradeId::new(502),
                maker_order_id: OrderId::new(10),
                taker_order_id: OrderId::new(100),
                price: Price::try_new(105).unwrap(),
                qty: Qty::try_new(5).unwrap(),
                maker_side: Side::Sell,
            },
        ];

        assert_eq!(trades, expected_trades);

        // maker 30 因完全成交被移除。
        // maker 10 因 Cancel 被移除。
        // 剩余订单保持原 source 顺序与 priority。
        let expected_book = vec![
            (order(20, Side::Buy, 95, 5), 5, QueuePriority::new(2)),
            (order(40, Side::Sell, 110, 6), 6, QueuePriority::new(3)),
        ];

        assert_eq!(final_book, expected_book);

        // 成交、移除和取消均不消耗新的 priority。
        assert_eq!(next_priority, 4);
    }
}

#[cfg(test)]
mod property_tests {
    //! 用生成式输入覆盖 FIFO、数量守恒、取消和失败原子性。
    use super::*;
    use matching_domain::UserId;
    use proptest::prelude::*;

    /// 构造生成式测试使用的合法订单 fixture。
    fn order(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
        LimitGtcOrder {
            order_id: OrderId::new(id),
            user_id: UserId::new(id as u64),
            side,
            price: Price::try_new(price).unwrap(),
            qty: Qty::try_new(qty).unwrap(),
        }
    }

    /// 保存源 `Vec` 顺序、原始订单、remaining 和 priority。
    fn snapshot(book: &ReferenceOrderBook) -> Vec<(LimitGtcOrder, u64, QueuePriority)> {
        book.orders
            .iter()
            .map(|maker| {
                (
                    maker.original_order().clone(),
                    maker.remaining(),
                    maker.priority(),
                )
            })
            .collect()
    }

    /// 将指定订单包装为尚未成交的 incoming 运行态。
    fn incoming(id: u128, side: Side, price: i64, qty: u64) -> IncomingOrder {
        IncomingOrder::new(order(id, side, price, qty))
    }

    proptest! {
        // 性质一：成交守恒、正成交量、同价 FIFO 前缀。
        #[test]
        fn prop_same_price_fifo_and_quantity_conservation(
            maker_qtys in prop::collection::vec(1u64..=32, 1..=8),
            taker_qty in 1u64..=256,
            maker_is_sell in any::<bool>(),
        ) {
            let maker_side = if maker_is_sell {
                Side::Sell
            } else {
                Side::Buy
            };
            let taker_side = if maker_is_sell {
                Side::Buy
            } else {
                Side::Sell
            };
            let maker_price = if maker_is_sell { 105 } else { 95 };
            let taker_price = if maker_is_sell { 110 } else { 90 };

            let mut book = ReferenceOrderBook::new();

            for (i, &qty) in maker_qtys.iter().enumerate() {
                book.rest(order(
                    i as u128 + 1,
                    maker_side,
                    maker_price,
                    qty,
                )).unwrap();
            }

            let before = snapshot(&book);
            let priority_before = book.next_priority;
            let total_maker: u64 = maker_qtys.iter().sum();

            let mut taker = incoming(
                100,
                taker_side,
                taker_price,
                taker_qty,
            );

            let fills = book.execute_fills(&mut taker).unwrap();

            // 期望成交量由生成的数据直接计算，
            // 不调用 planner 或其他撮合算法。
            let expected_total = taker_qty.min(total_maker);
            let actual_total: u64 =
                fills.iter().map(|(_, _, qty)| qty.get()).sum();

            prop_assert_eq!(actual_total, expected_total);
            prop_assert_eq!(
                actual_total + taker.remaining(),
                taker_qty
            );
            prop_assert_eq!(
                taker.original_order().qty.get(),
                taker_qty
            );

            // 手工构建 FIFO 前缀及每张 maker 的期望成交量。
            let mut unfilled = taker_qty;
            let mut expected_fills = Vec::new();
            let mut expected_book = Vec::new();

            for (i, &(ref original, remaining, priority))
                in before.iter().enumerate()
            {
                let filled = remaining.min(unfilled);

                if filled > 0 {
                    expected_fills.push((
                        original.order_id,
                        Price::try_new(maker_price).unwrap(),
                        Qty::try_new(filled).unwrap(),
                    ));
                    unfilled -= filled;
                }

                let maker_remaining = remaining - filled;
                if maker_remaining > 0 {
                    expected_book.push((
                        original.clone(),
                        maker_remaining,
                        priority,
                    ));
                }

                prop_assert_eq!(
                    original.order_id,
                    OrderId::new(i as u128 + 1)
                );
            }

            // 同时验证 FIFO 前缀、每笔数量及 maker-price。
            prop_assert_eq!(fills, expected_fills);

            // 完全成交的 maker 被移除；
            // 未耗尽的 maker 保持 source 顺序及 priority。
            prop_assert_eq!(snapshot(&book), expected_book);
            prop_assert_eq!(book.next_priority, priority_before);
        }

        // 性质二：任意有效位置取消后的完整状态。
        #[test]
        fn prop_cancel_valid_position_preserves_remaining_book(
            quantities in prop::collection::vec(1u64..=100, 1..=8),
            index_seed in any::<usize>(),
            is_sell in any::<bool>(),
        ) {
            let side = if is_sell {
                Side::Sell
            } else {
                Side::Buy
            };

            let mut book = ReferenceOrderBook::new();

            for (i, &qty) in quantities.iter().enumerate() {
                book.rest(order(
                    i as u128 + 1,
                    side,
                    100,
                    qty,
                )).unwrap();

                // 预先制造不同的 maker remaining，
                // 同时保留 Qty 严格正值约束。
                let filled = qty / 2;
                if filled > 0 {
                    book.orders[i]
                        .apply_fill(Qty::try_new(filled).unwrap())
                        .unwrap();
                }
            }

            let before = snapshot(&book);
            let priority_before = book.next_priority;
            let target_index = index_seed % quantities.len();
            let target_id = before[target_index].0.order_id;

            // 期望状态仅通过过滤取消前的快照生成。
            let expected_remaining: Vec<_> = before
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != target_index)
                .map(|(_, state)| state.clone())
                .collect();

            let expected_canceled = before[target_index].clone();

            let canceled = book.cancel(target_id).unwrap();

            prop_assert_eq!(
                canceled.original_order(),
                &expected_canceled.0
            );
            prop_assert_eq!(
                canceled.remaining(),
                expected_canceled.1
            );
            prop_assert_eq!(
                canceled.priority(),
                expected_canceled.2
            );

            // 完整快照同时验证 source/FIFO 顺序、
            // 原始订单、remaining 和 QueuePriority。
            prop_assert_eq!(snapshot(&book), expected_remaining);
                let target_removed = book
            .orders
            .iter()
            .all(|resting| resting.original_order().order_id != target_id);

            prop_assert!(
                target_removed,
                "canceled order must not remain in the book"
            );

            prop_assert_eq!(
                book.orders.len(),
                quantities.len() - 1
            );
            prop_assert_eq!(
                book.next_priority,
                priority_before
            );
        }
    }
}
