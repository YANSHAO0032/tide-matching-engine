//! 生产簿普通挂单的二级到期索引与确定性到期入口。

use super::ProductionOrderBook;
use matching_domain::{
    EngineEvent, ExpireReason, ExpiredOrder, LimitGtcOrder, LogicalTime, OrderId, RejectReason,
    TimeInForce, TradeId, TriggerExpiry,
};
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
#[path = "expiry/test_support.rs"]
pub(crate) mod test_support;

/// 普通挂单的二级到期索引。
///
/// 两个 `BTreeMap` 的键顺序和每个 `BTreeSet` 的标识顺序共同冻结到期输出，
/// 业务结果不依赖 Arena 槽位、先进先出链接或 HashMap 迭代顺序。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct RegularExpiryIndex {
    /// 按到期逻辑时间索引的 GTD 普通订单身份集合。
    gtd: BTreeMap<LogicalTime, BTreeSet<OrderId>>,
    /// 按交易时段索引的 DAY 普通订单身份集合。
    day: BTreeMap<u64, BTreeSet<OrderId>>,
}

impl RegularExpiryIndex {
    pub(super) fn insert(&mut self, order_id: OrderId, tif: TimeInForce) {
        match tif {
            TimeInForce::Gtc => {}
            TimeInForce::Gtd { expire_at } => {
                self.gtd.entry(expire_at).or_default().insert(order_id);
            }
            TimeInForce::Day { session_id } => {
                self.day.entry(session_id).or_default().insert(order_id);
            }
            TimeInForce::Ioc | TimeInForce::Fok => {
                panic!(
                    "ProductionOrderBook invariant violation: immediate TIF in regular expiry index"
                )
            }
        }
    }

    pub(super) fn remove(&mut self, order_id: OrderId, tif: TimeInForce) {
        let (key, is_gtd) = match tif {
            TimeInForce::Gtc => return,
            TimeInForce::Gtd { expire_at } => (expire_at.get(), true),
            TimeInForce::Day { session_id } => (session_id, false),
            TimeInForce::Ioc | TimeInForce::Fok => return,
        };
        if is_gtd {
            let logical = LogicalTime::new(key);
            if let Some(entries) = self.gtd.get_mut(&logical) {
                entries.remove(&order_id);
                if entries.is_empty() {
                    self.gtd.remove(&logical);
                }
            }
        } else if let Some(entries) = self.day.get_mut(&key) {
            entries.remove(&order_id);
            if entries.is_empty() {
                self.day.remove(&key);
            }
        }
    }

    fn take_gtd_through(&mut self, now: LogicalTime) -> Vec<(LogicalTime, OrderId)> {
        let keys = self
            .gtd
            .range(..=now)
            .map(|(time, _)| *time)
            .collect::<Vec<_>>();
        keys.into_iter()
            .flat_map(|time| {
                self.gtd
                    .remove(&time)
                    .into_iter()
                    .flatten()
                    .map(move |order_id| (time, order_id))
            })
            .collect()
    }

    fn take_day_session(&mut self, session_id: u64) -> Vec<OrderId> {
        self.day.remove(&session_id).into_iter().flatten().collect()
    }
}

impl ProductionOrderBook {
    /// 执行带 DAY/GTD/GTC 存活期的普通限价单，并仅为实际余量登记到期状态。
    pub fn place_limit_timed_with_events(
        &mut self,
        order: LimitGtcOrder,
        tif: TimeInForce,
        now: LogicalTime,
        current_session_id: u64,
        trade_ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, RejectReason> {
        if self.contains_active_order(order.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        Self::validate_resting_tif(tif, now, current_session_id)?;
        let order_id = order.order_id;
        let events = self.place_limit_gtc_with_events(order, trade_ids)?;
        if let Some(index) = self.orders.get(&order_id).copied() {
            let node = self.arena.get_mut(index).unwrap_or_else(|| {
                panic!("ProductionOrderBook invariant violation: timed residual slot missing")
            });
            node.replace_initial_gtc_with_tif(tif);
            self.regular_expiry.insert(order_id, tif);
        }
        Ok(events)
    }

    /// 使普通 GTD 挂单在到达给定逻辑时间后确定性到期。
    pub fn expire_gtd_through(&mut self, now: LogicalTime) -> Vec<ExpiredOrder> {
        let mut expired = self
            .regular_expiry
            .take_gtd_through(now)
            .into_iter()
            .map(|(expire_at, order_id)| {
                self.cancel(order_id).unwrap_or_else(|reason| {
                    panic!("ProductionOrderBook invariant violation: indexed GTD order missing: {reason:?}")
                });
                (
                    expire_at,
                    ExpiredOrder {
                        order_id,
                        reason: ExpireReason::GtdReached,
                    },
                )
            })
            .collect::<Vec<_>>();
        expired.extend(self.expire_stop_gtd_through(now).into_iter().map(
            |(order, _)| match order.trigger_expiry() {
                TriggerExpiry::Gtd { expire_at } => (
                    expire_at,
                    ExpiredOrder {
                        order_id: order.order_id(),
                        reason: ExpireReason::StopTriggerGtdReached,
                    },
                ),
                TriggerExpiry::Gtc | TriggerExpiry::Day { .. } => panic!(
                    "ProductionOrderBook invariant violation: non-GTD stop returned by GTD expiry"
                ),
            },
        ));
        expired.sort_unstable_by_key(|(expire_at, entry)| (*expire_at, entry.order_id));
        expired.into_iter().map(|(_, entry)| entry).collect()
    }

    /// 终结当前交易时段中的普通 DAY 挂单。
    pub fn end_session(
        &mut self,
        session_id: u64,
        current_session_id: u64,
    ) -> Result<Vec<ExpiredOrder>, RejectReason> {
        if session_id != current_session_id {
            return Err(RejectReason::InvalidSession);
        }
        let mut expired = self
            .regular_expiry
            .take_day_session(session_id)
            .into_iter()
            .map(|order_id| {
                self.cancel(order_id).unwrap_or_else(|reason| {
                    panic!("ProductionOrderBook invariant violation: indexed DAY order missing: {reason:?}")
                });
                ExpiredOrder {
                    order_id,
                    reason: ExpireReason::SessionEnded,
                }
            })
            .collect::<Vec<_>>();
        expired.extend(
            self.expire_stop_day_session(session_id)
                .into_iter()
                .map(|(order, _)| ExpiredOrder {
                    order_id: order.order_id(),
                    reason: ExpireReason::StopTriggerSessionEnded,
                }),
        );
        expired.sort_unstable_by_key(|entry| entry.order_id);
        Ok(expired)
    }
}
