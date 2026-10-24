use std::collections::{BTreeMap, HashMap};

use matching_domain::{
    LogicalTime, OrderId, Price, RejectReason, Side, StopMarketQuantity, StopOrder, StopPriority,
    TimeInForce, TriggerExpiry,
};

use crate::StopPriorityAllocator;

/// 止损订单在一侧 StopBook 中的价位位置。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StopLocation {
    /// 未触发订单所属的买卖方向。
    side: Side,
    /// 未触发订单所属的触发价格档位。
    stop_price: Price,
}

/// 到期索引中保存的稳定订单引用。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ExpiryRef {
    /// 被索引的未触发订单标识。
    order_id: OrderId,
    /// 删除时用于复核订单仍位于原始止损档位的位置。
    location: StopLocation,
}

/// 未触发止损订单及其独立触发优先级。
#[derive(Clone, Debug, PartialEq, Eq)]
struct StopEntry {
    /// 未触发的原始止损订单。
    order: StopOrder,
    /// 仅用于同一止损价位先进先出的独立优先级。
    priority: StopPriority,
}

/// 未触发止损订单的统一到期索引。
///
/// GTD 按逻辑时间、DAY 按交易时段登记；两种索引使用相同的订单引用形式。
#[derive(Clone, Debug, Default)]
struct StopExpiryIndex {
    /// 按逻辑时间升序保存的 GTD 到期引用。
    gtd: BTreeMap<LogicalTime, Vec<ExpiryRef>>,
    /// 按交易时段升序保存的 DAY 到期引用。
    day: BTreeMap<u64, Vec<ExpiryRef>>,
}

impl StopExpiryIndex {
    /// 将需要在触发前到期的订单登记到对应桶。
    fn insert(&mut self, expiry: TriggerExpiry, reference: ExpiryRef) {
        match expiry {
            TriggerExpiry::Gtc => {}
            TriggerExpiry::Day { session_id } => {
                self.day.entry(session_id).or_default().push(reference)
            }
            TriggerExpiry::Gtd { expire_at } => {
                self.gtd.entry(expire_at).or_default().push(reference)
            }
        }
    }

    /// 删除一个仍在索引中的订单引用。
    fn remove(&mut self, expiry: TriggerExpiry, order_id: OrderId) {
        match expiry {
            TriggerExpiry::Gtc => {}
            TriggerExpiry::Day { session_id } => {
                Self::remove_from(&mut self.day, session_id, order_id)
            }
            TriggerExpiry::Gtd { expire_at } => {
                Self::remove_from(&mut self.gtd, expire_at, order_id)
            }
        }
    }

    /// 从索引桶删除引用，并清理空桶。
    fn remove_from<K: Ord + Copy>(
        index: &mut BTreeMap<K, Vec<ExpiryRef>>,
        key: K,
        order_id: OrderId,
    ) {
        let empty = if let Some(references) = index.get_mut(&key) {
            references.retain(|reference| reference.order_id != order_id);
            references.is_empty()
        } else {
            false
        };
        if empty {
            index.remove(&key);
        }
    }

    /// 取出时间不晚于当前逻辑时间的全部 GTD 引用。
    fn take_gtd_through(&mut self, now: LogicalTime) -> Vec<ExpiryRef> {
        let keys = self
            .gtd
            .range(..=now)
            .map(|(key, _)| *key)
            .collect::<Vec<_>>();
        keys.into_iter()
            .filter_map(|key| self.gtd.remove(&key))
            .flatten()
            .collect()
    }

    /// 取出指定交易时段的全部 DAY 引用。
    fn take_day_session(&mut self, session_id: u64) -> Vec<ExpiryRef> {
        self.day.remove(&session_id).unwrap_or_default()
    }
}

/// 以触发价位保存未触发订单的 StopBook。
///
/// 买方遍历为低触发价到高触发价，卖方遍历为高触发价到低触发价。同一价位按
/// StopPriority 严格递增；未触发订单不拥有 QueuePriority。
#[derive(Clone, Debug, Default)]
pub struct StopBook {
    /// 买方向止损价位表。
    buy_stops: BTreeMap<Price, Vec<StopEntry>>,
    /// 卖方向止损价位表。
    sell_stops: BTreeMap<Price, Vec<StopEntry>>,
    /// 订单标识到止损方向及价位的定位索引。
    locations: HashMap<OrderId, StopLocation>,
    /// 未触发订单的统一触发前到期索引。
    expiry_index: StopExpiryIndex,
}

impl StopBook {
    /// 创建空的 StopBook。
    pub fn new() -> Self {
        Self::default()
    }

    /// 判断标识是否属于一个未触发止损订单。
    pub fn contains(&self, order_id: OrderId) -> bool {
        self.locations.contains_key(&order_id)
    }

    /// 校验并加入一个未触发止损订单。
    pub fn insert(
        &mut self,
        order: StopOrder,
        now: LogicalTime,
        current_session_id: u64,
        priorities: &mut StopPriorityAllocator,
    ) -> Result<StopPriority, RejectReason> {
        let order_id = order.order_id();
        if self.contains(order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        Self::validate_child_tif(&order)?;
        Self::validate_expiry(order.trigger_expiry(), now, current_session_id)?;
        priorities.preflight_allocate()?;

        let location = StopLocation {
            side: order.side(),
            stop_price: order.stop_price(),
        };
        let expiry = order.trigger_expiry();
        let priority = priorities.allocate()?;
        self.levels_mut(location.side)
            .entry(location.stop_price)
            .or_default()
            .push(StopEntry { order, priority });
        self.locations.insert(order_id, location);
        self.expiry_index
            .insert(expiry, ExpiryRef { order_id, location });
        debug_assert!(self.validate().is_ok());
        Ok(priority)
    }

    /// 查询未触发订单及其 StopPriority。
    pub fn get(&self, order_id: OrderId) -> Option<(&StopOrder, StopPriority)> {
        let entry = self.entry(order_id)?;
        Some((&entry.order, entry.priority))
    }

    /// 以规定的触发顺序返回一侧订单快照。
    pub fn ordered(&self, side: Side) -> Vec<(&StopOrder, StopPriority)> {
        let mut result = Vec::new();
        match side {
            Side::Buy => self.buy_stops.values().for_each(|entries| {
                result.extend(entries.iter().map(|entry| (&entry.order, entry.priority)));
            }),
            Side::Sell => self.sell_stops.values().rev().for_each(|entries| {
                result.extend(entries.iter().map(|entry| (&entry.order, entry.priority)));
            }),
        }
        result
    }

    /// 按一次实际价格跨越的方向、价格和 FIFO 顺序终结未触发订单。
    ///
    /// 买方只在价格上升且 `previous < stop_price <= current` 时触发；卖方只在
    /// 价格下降且 `current <= stop_price < previous` 时触发。订单先从 StopBook
    /// 和到期索引删除，调用方才能执行 child。
    pub fn take_triggered_crossing(
        &mut self,
        side: Side,
        previous: Price,
        current: Price,
    ) -> Vec<(StopOrder, StopPriority)> {
        let crossed = match side {
            Side::Buy => current > previous,
            Side::Sell => current < previous,
        };
        if !crossed {
            return Vec::new();
        }
        let order_ids = self
            .ordered(side)
            .into_iter()
            .filter(|(order, _)| match side {
                Side::Buy => previous < order.stop_price() && order.stop_price() <= current,
                Side::Sell => current <= order.stop_price() && order.stop_price() < previous,
            })
            .map(|(order, _)| order.order_id())
            .collect::<Vec<_>>();
        let triggered = order_ids
            .into_iter()
            .filter_map(|order_id| self.remove(order_id))
            .collect::<Vec<_>>();
        debug_assert!(self.validate().is_ok());
        triggered
    }

    /// 删除一个未触发止损订单。
    pub fn remove(&mut self, order_id: OrderId) -> Option<(StopOrder, StopPriority)> {
        let entry = self.remove_inner(order_id)?;
        self.expiry_index.remove(entry.0.trigger_expiry(), order_id);
        debug_assert!(self.validate().is_ok());
        Some(entry)
    }

    /// 删除不晚于当前逻辑时间的 GTD 未触发订单。
    pub fn expire_gtd_through(&mut self, now: LogicalTime) -> Vec<(StopOrder, StopPriority)> {
        let references = self.expiry_index.take_gtd_through(now);
        self.expire_references(references)
    }

    /// 删除指定交易时段结束时仍未触发的 DAY 订单。
    pub fn expire_day_session(&mut self, session_id: u64) -> Vec<(StopOrder, StopPriority)> {
        let references = self.expiry_index.take_day_session(session_id);
        self.expire_references(references)
    }

    /// 检查主容器、位置索引、到期索引与同价 FIFO 的一致性。
    pub fn validate(&self) -> Result<(), &'static str> {
        let mut level_seen = HashMap::new();
        self.validate_side(Side::Buy, &self.buy_stops, &mut level_seen)?;
        self.validate_side(Side::Sell, &self.sell_stops, &mut level_seen)?;
        if level_seen.len() != self.locations.len() {
            return Err("止损位置索引数量不一致");
        }
        let mut expiry_seen = HashMap::new();
        for (time, references) in &self.expiry_index.gtd {
            for reference in references {
                self.validate_expiry_reference(
                    *reference,
                    TriggerExpiry::Gtd { expire_at: *time },
                    &mut expiry_seen,
                )?;
            }
        }
        for (session_id, references) in &self.expiry_index.day {
            for reference in references {
                self.validate_expiry_reference(
                    *reference,
                    TriggerExpiry::Day {
                        session_id: *session_id,
                    },
                    &mut expiry_seen,
                )?;
            }
        }
        for order_id in self.locations.keys() {
            let entry = self.entry(*order_id).ok_or("止损位置索引指向不存在订单")?;
            if !matches!(entry.order.trigger_expiry(), TriggerExpiry::Gtc)
                && !expiry_seen.contains_key(order_id)
            {
                return Err("需要到期的止损订单缺少索引引用");
            }
        }
        Ok(())
    }

    /// 校验触发前到期条件。
    fn validate_expiry(
        expiry: TriggerExpiry,
        now: LogicalTime,
        current_session_id: u64,
    ) -> Result<(), RejectReason> {
        match expiry {
            TriggerExpiry::Gtc => Ok(()),
            TriggerExpiry::Day { session_id } if session_id == current_session_id => Ok(()),
            TriggerExpiry::Day { .. } => Err(RejectReason::InvalidExpireTime),
            TriggerExpiry::Gtd { expire_at } if expire_at > now => Ok(()),
            TriggerExpiry::Gtd { .. } => Err(RejectReason::InvalidExpireTime),
        }
    }

    /// 校验止损触发后 child 的可实现时效组合。
    ///
    /// 接收阶段允许 StopLimit 携带 GTC、DAY 或 GTD child，StopMarket 只接受
    /// 不挂单的 IOC child。触发阶段会再次校验当前实际可执行的组合；尚未实现
    /// 的 DAY/GTD 常规生命周期会在 Trigger-R1 前被拒绝。FOK child 留待具备
    /// 完整触发预检时再开放。
    fn validate_child_tif(order: &StopOrder) -> Result<(), RejectReason> {
        match order {
            StopOrder::Limit(order)
                if matches!(
                    order.child_tif,
                    TimeInForce::Gtc | TimeInForce::Day { .. } | TimeInForce::Gtd { .. }
                ) =>
            {
                Ok(())
            }
            StopOrder::Market(order)
                if matches!(
                    (&order.quantity, order.child_tif),
                    (
                        StopMarketQuantity::Base(_) | StopMarketQuantity::Quote(_),
                        TimeInForce::Ioc
                    )
                ) =>
            {
                Ok(())
            }
            StopOrder::Limit(_) | StopOrder::Market(_) => {
                Err(RejectReason::InvalidOrderCombination)
            }
        }
    }

    /// 从主容器删除订单，不更新已经被取出的到期索引桶。
    fn remove_inner(&mut self, order_id: OrderId) -> Option<(StopOrder, StopPriority)> {
        let location = self.locations.remove(&order_id)?;
        let entries = self
            .levels_mut(location.side)
            .get_mut(&location.stop_price)
            .expect("StopBook 位置索引必须指向存在的止损价位");
        let position = entries
            .iter()
            .position(|entry| entry.order.order_id() == order_id)
            .expect("StopBook 位置索引必须指向价位中的订单");
        let entry = entries.remove(position);
        if entries.is_empty() {
            self.levels_mut(location.side).remove(&location.stop_price);
        }
        Some((entry.order, entry.priority))
    }

    /// 根据已经取出的索引引用删除仍然存在的订单。
    fn expire_references(&mut self, references: Vec<ExpiryRef>) -> Vec<(StopOrder, StopPriority)> {
        let mut expired = Vec::with_capacity(references.len());
        for reference in references {
            if self.locations.get(&reference.order_id) == Some(&reference.location)
                && let Some(entry) = self.remove_inner(reference.order_id)
            {
                expired.push(entry);
            }
        }
        debug_assert!(self.validate().is_ok());
        expired
    }

    /// 查询内部条目。
    fn entry(&self, order_id: OrderId) -> Option<&StopEntry> {
        let location = self.locations.get(&order_id)?;
        self.levels(location.side)
            .get(&location.stop_price)?
            .iter()
            .find(|entry| entry.order.order_id() == order_id)
    }

    /// 选择一侧价位表。
    fn levels(&self, side: Side) -> &BTreeMap<Price, Vec<StopEntry>> {
        match side {
            Side::Buy => &self.buy_stops,
            Side::Sell => &self.sell_stops,
        }
    }

    /// 可变地选择一侧价位表。
    fn levels_mut(&mut self, side: Side) -> &mut BTreeMap<Price, Vec<StopEntry>> {
        match side {
            Side::Buy => &mut self.buy_stops,
            Side::Sell => &mut self.sell_stops,
        }
    }

    /// 校验一个方向的价位与位置索引。
    fn validate_side(
        &self,
        side: Side,
        levels: &BTreeMap<Price, Vec<StopEntry>>,
        seen: &mut HashMap<OrderId, StopLocation>,
    ) -> Result<(), &'static str> {
        for (stop_price, entries) in levels {
            let mut previous = None;
            for entry in entries {
                let location = StopLocation {
                    side,
                    stop_price: *stop_price,
                };
                if entry.order.side() != side
                    || entry.order.stop_price() != *stop_price
                    || self.locations.get(&entry.order.order_id()) != Some(&location)
                {
                    return Err("止损订单价位或位置索引不一致");
                }
                if previous.is_some_and(|priority| entry.priority <= priority) {
                    return Err("同一止损价位的 StopPriority 未递增");
                }
                previous = Some(entry.priority);
                if seen.insert(entry.order.order_id(), location).is_some() {
                    return Err("止损订单重复出现在多个价位");
                }
            }
        }
        Ok(())
    }

    /// 校验一条到期索引引用。
    fn validate_expiry_reference(
        &self,
        reference: ExpiryRef,
        expected: TriggerExpiry,
        seen: &mut HashMap<OrderId, StopLocation>,
    ) -> Result<(), &'static str> {
        let entry = self
            .entry(reference.order_id)
            .ok_or("到期索引指向不存在订单")?;
        if self.locations.get(&reference.order_id) != Some(&reference.location)
            || entry.order.trigger_expiry() != expected
            || seen
                .insert(reference.order_id, reference.location)
                .is_some()
        {
            return Err("止损到期索引不一致");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use matching_domain::{
        LimitGtcOrder, LogicalTime, OrderId, Price, Qty, QuoteBudget, RejectReason, ReservationId,
        Side, StopLimitOrder, StopMarketOrder, StopMarketQuantity, StopOrder, TimeInForce,
        TriggerExpiry, UserId,
    };

    use crate::{
        ProductionOrderBook, QueuePriorityPolicy, ReferenceOrderBook, StopBook,
        StopPriorityAllocator,
    };

    fn limit(id: u64, side: Side, stop: u64, expiry: TriggerExpiry) -> StopOrder {
        StopOrder::Limit(StopLimitOrder {
            order_id: OrderId::new(u128::from(id)),
            user_id: UserId::new(1),
            reservation_id: ReservationId::new(u128::from(id)),
            fee_schedule_version: 1,
            side,
            stop_price: Price::try_new(i64::try_from(stop).unwrap()).unwrap(),
            price: Price::try_new(i64::try_from(stop + 1).unwrap()).unwrap(),
            qty: Qty::try_new(2).unwrap(),
            child_tif: TimeInForce::Gtc,
            trigger_expiry: expiry,
        })
    }

    fn market(id: u64, side: Side, stop: u64, expiry: TriggerExpiry) -> StopOrder {
        StopOrder::Market(StopMarketOrder {
            order_id: OrderId::new(u128::from(id)),
            user_id: UserId::new(2),
            reservation_id: ReservationId::new(u128::from(id)),
            fee_schedule_version: 1,
            side,
            stop_price: Price::try_new(i64::try_from(stop).unwrap()).unwrap(),
            quantity: StopMarketQuantity::Quote(QuoteBudget::try_new(10).unwrap()),
            child_tif: TimeInForce::Ioc,
            trigger_expiry: expiry,
        })
    }

    fn allocator() -> StopPriorityAllocator {
        StopPriorityAllocator::new(QueuePriorityPolicy::try_new(1024).unwrap())
    }

    #[test]
    fn same_trigger_price_obeys_stop_priority_fifo() {
        let mut book = StopBook::new();
        let mut priorities = allocator();
        let now = LogicalTime::new(40);
        book.insert(
            limit(1, Side::Buy, 100, TriggerExpiry::Gtc),
            now,
            3,
            &mut priorities,
        )
        .unwrap();
        book.insert(
            market(2, Side::Buy, 100, TriggerExpiry::Gtc),
            now,
            3,
            &mut priorities,
        )
        .unwrap();
        book.insert(
            limit(3, Side::Buy, 90, TriggerExpiry::Gtc),
            now,
            3,
            &mut priorities,
        )
        .unwrap();
        assert_eq!(
            book.ordered(Side::Buy)
                .iter()
                .map(|(order, _)| order.order_id().get())
                .collect::<Vec<_>>(),
            vec![3, 1, 2]
        );
        assert!(book.validate().is_ok());
    }

    #[test]
    fn gtd_and_day_expire_from_separate_pretrigger_indices() {
        let mut book = StopBook::new();
        let mut priorities = allocator();
        let now = LogicalTime::new(40);
        book.insert(
            limit(
                1,
                Side::Sell,
                100,
                TriggerExpiry::Gtd {
                    expire_at: LogicalTime::new(42),
                },
            ),
            now,
            3,
            &mut priorities,
        )
        .unwrap();
        book.insert(
            limit(2, Side::Sell, 90, TriggerExpiry::Day { session_id: 3 }),
            now,
            3,
            &mut priorities,
        )
        .unwrap();
        book.insert(
            limit(3, Side::Sell, 80, TriggerExpiry::Gtc),
            now,
            3,
            &mut priorities,
        )
        .unwrap();
        assert!(book.expire_gtd_through(LogicalTime::new(41)).is_empty());
        assert_eq!(
            book.expire_gtd_through(LogicalTime::new(42))[0]
                .0
                .order_id()
                .get(),
            1
        );
        assert_eq!(book.expire_day_session(3)[0].0.order_id().get(), 2);
        assert!(book.contains(OrderId::new(3)));
        assert!(book.validate().is_ok());
    }

    #[test]
    fn invalid_pretrigger_expiry_does_not_consume_stop_priority_or_mutate_book() {
        let mut book = StopBook::new();
        let mut priorities = allocator();
        let now = LogicalTime::new(40);
        assert_eq!(
            book.insert(
                limit(1, Side::Buy, 100, TriggerExpiry::Gtd { expire_at: now }),
                now,
                3,
                &mut priorities
            ),
            Err(RejectReason::InvalidExpireTime)
        );
        assert_eq!(priorities.next(), 0);
        assert!(!book.contains(OrderId::new(1)));
    }

    #[test]
    fn stop_priority_exhaustion_rejects_without_mutating_stop_book() {
        let policy = QueuePriorityPolicy::try_with_synthetic_ceiling(1, Some(1)).unwrap();
        let mut book = StopBook::new();
        let mut priorities = StopPriorityAllocator::new(policy);
        let now = LogicalTime::new(40);
        book.insert(
            limit(1, Side::Buy, 100, TriggerExpiry::Gtc),
            now,
            3,
            &mut priorities,
        )
        .unwrap();
        assert_eq!(
            book.insert(
                limit(2, Side::Buy, 100, TriggerExpiry::Gtc),
                now,
                3,
                &mut priorities
            ),
            Err(RejectReason::PrioritySpaceExhaustion)
        );
        assert_eq!(priorities.next(), 1);
        assert!(!book.contains(OrderId::new(2)));
        assert!(book.validate().is_ok());
    }

    #[test]
    fn production_and_reference_stop_state_match_and_order_ids_are_globally_unique() {
        let policy = QueuePriorityPolicy::try_new(1024).unwrap();
        let mut production = ProductionOrderBook::new(policy);
        let mut reference = ReferenceOrderBook::new();
        let now = LogicalTime::new(50);
        let orders = [
            limit(1, Side::Buy, 120, TriggerExpiry::Gtc),
            market(
                2,
                Side::Buy,
                100,
                TriggerExpiry::Gtd {
                    expire_at: LogicalTime::new(55),
                },
            ),
            limit(3, Side::Sell, 90, TriggerExpiry::Day { session_id: 8 }),
        ];
        for order in orders {
            assert_eq!(
                production.place_stop(order.clone(), now, 8),
                reference.place_stop(order, now, 8)
            );
        }
        for side in [Side::Buy, Side::Sell] {
            let production_view = production
                .stop_orders(side)
                .into_iter()
                .map(|(order, priority)| (order.order_id(), priority))
                .collect::<Vec<_>>();
            let reference_view = reference
                .stop_orders(side)
                .into_iter()
                .map(|(order, priority)| (order.order_id(), priority))
                .collect::<Vec<_>>();
            assert_eq!(production_view, reference_view);
        }
        assert_eq!(
            production.expire_stop_gtd_through(LogicalTime::new(55)),
            reference.expire_stop_gtd_through(LogicalTime::new(55))
        );
        assert_eq!(
            production.expire_stop_day_session(8),
            reference.expire_stop_day_session(8)
        );
        assert!(production.validate_stop_book().is_ok());

        let regular = LimitGtcOrder {
            order_id: OrderId::new(1),
            user_id: UserId::new(3),
            side: Side::Buy,
            price: Price::try_new(10).unwrap(),
            qty: Qty::try_new(1).unwrap(),
        };
        assert_eq!(
            production.rest(regular.clone()),
            Err(RejectReason::DuplicateOrderId)
        );
        assert_eq!(reference.rest(regular), Err(RejectReason::DuplicateOrderId));
    }

    proptest! {
        #[test]
        fn arbitrary_untriggered_stop_sequences_preserve_production_reference_trigger_order(
            entries in proptest::collection::vec((any::<bool>(), 1_u64..1_000_u64), 0..32)
        ) {
            let policy = QueuePriorityPolicy::try_new(1024).unwrap();
            let mut production = ProductionOrderBook::new(policy);
            let mut reference = ReferenceOrderBook::new();
            let now = LogicalTime::new(70);

            for (offset, (is_buy, stop_price)) in entries.into_iter().enumerate() {
                let side = if is_buy { Side::Buy } else { Side::Sell };
                let order = limit(
                    u64::try_from(offset).unwrap() + 1,
                    side,
                    stop_price,
                    TriggerExpiry::Gtc,
                );
                prop_assert_eq!(
                    production.place_stop(order.clone(), now, 2),
                    reference.place_stop(order, now, 2)
                );
            }

            for side in [Side::Buy, Side::Sell] {
                let production_view = production
                    .stop_orders(side)
                    .into_iter()
                    .map(|(order, priority)| (order.order_id(), priority))
                    .collect::<Vec<_>>();
                let reference_view = reference
                    .stop_orders(side)
                    .into_iter()
                    .map(|(order, priority)| (order.order_id(), priority))
                    .collect::<Vec<_>>();
                prop_assert_eq!(production_view, reference_view);
            }
            prop_assert!(production.validate_stop_book().is_ok());
        }
    }
}
