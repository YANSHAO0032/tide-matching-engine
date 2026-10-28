//! 止损簿跨模块测试所需的确定性状态投影。

use super::StopBook;
use matching_domain::{OrderId, Price, PriceReferenceKind, Side, TriggerExpiry};

impl StopBook {
    /// 测试读取真实到期桶引用及来源位置；业务队列与到期事件不经此排序。
    pub(crate) fn test_expiry_entries(
        &self,
    ) -> Vec<(OrderId, TriggerExpiry, PriceReferenceKind, Side, Price)> {
        let mut entries = self
            .expiry_index
            .gtd
            .iter()
            .flat_map(|(at, refs)| {
                refs.iter().map(move |r| {
                    (
                        r.order_id,
                        TriggerExpiry::Gtd { expire_at: *at },
                        r.location.reference,
                        r.location.side,
                        r.location.stop_price,
                    )
                })
            })
            .chain(self.expiry_index.day.iter().flat_map(|(session, refs)| {
                refs.iter().map(move |r| {
                    (
                        r.order_id,
                        TriggerExpiry::Day {
                            session_id: *session,
                        },
                        r.location.reference,
                        r.location.side,
                        r.location.stop_price,
                    )
                })
            }))
            .collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(id, ..)| *id);
        entries
    }

    /// 测试相等投影显式排序身份索引，保留实际触发队列与到期引用原序。
    pub(crate) fn test_deterministic_state(&self) -> String {
        let mut locations = self.locations.iter().collect::<Vec<_>>();
        locations.sort_unstable_by_key(|(id, _)| **id);
        format!(
            "{:?}",
            (
                &self.buy_stops,
                &self.sell_stops,
                locations,
                &self.expiry_index
            )
        )
    }
}
