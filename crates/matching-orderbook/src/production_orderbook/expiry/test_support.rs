//! 普通订单到期索引的测试投影和完整性校验。
//!
//! 此文件仅在测试构建中编译；它读取真实状态，不参与到期执行路径。

use super::super::ProductionOrderBook;
use super::RegularExpiryIndex;
use matching_domain::{OrderId, TimeInForce};
use std::collections::BTreeSet;

impl RegularExpiryIndex {
    /// 差分测试读取真实二级索引；按身份排序只统一两模型不同存储形式。
    pub(crate) fn test_entries(&self) -> Vec<(OrderId, TimeInForce)> {
        let mut entries = self
            .gtd
            .iter()
            .flat_map(|(at, ids)| {
                ids.iter()
                    .map(move |id| (*id, TimeInForce::Gtd { expire_at: *at }))
            })
            .chain(self.day.iter().flat_map(|(session, ids)| {
                ids.iter().map(move |id| {
                    (
                        *id,
                        TimeInForce::Day {
                            session_id: *session,
                        },
                    )
                })
            }))
            .collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(id, _)| *id);
        entries
    }
}

impl ProductionOrderBook {
    /// 只读校验普通挂单到期索引与活动订单的一一对应关系。
    pub(crate) fn validate_regular_expiry(&self) -> Result<(), &'static str> {
        let mut seen = BTreeSet::new();
        for (expire_at, order_ids) in &self.regular_expiry.gtd {
            for order_id in order_ids {
                let index = self.orders.get(order_id).ok_or("GTD 索引引用不存在订单")?;
                let node = self.arena.get(*index).ok_or("GTD 索引引用不存在槽位")?;
                if node.time_in_force()
                    != (TimeInForce::Gtd {
                        expire_at: *expire_at,
                    })
                {
                    return Err("GTD 索引时间与订单有效期不一致");
                }
                if !seen.insert(*order_id) {
                    return Err("普通到期索引存在重复订单引用");
                }
            }
        }
        for (session_id, order_ids) in &self.regular_expiry.day {
            for order_id in order_ids {
                let index = self.orders.get(order_id).ok_or("DAY 索引引用不存在订单")?;
                let node = self.arena.get(*index).ok_or("DAY 索引引用不存在槽位")?;
                if node.time_in_force()
                    != (TimeInForce::Day {
                        session_id: *session_id,
                    })
                {
                    return Err("DAY 索引会话与订单有效期不一致");
                }
                if !seen.insert(*order_id) {
                    return Err("普通到期索引存在重复订单引用");
                }
            }
        }
        for (order_id, index) in &self.orders {
            let node = self.arena.get(*index).ok_or("订单索引引用不存在槽位")?;
            match node.time_in_force() {
                TimeInForce::Gtc => {
                    if seen.contains(order_id) {
                        return Err("GTC 订单错误出现在到期索引");
                    }
                }
                TimeInForce::Gtd { .. } | TimeInForce::Day { .. } => {
                    if !seen.contains(order_id) {
                        return Err("活动定时订单缺少到期索引引用");
                    }
                }
                TimeInForce::Ioc | TimeInForce::Fok => {
                    return Err("立即有效期订单错误留在常规订单簿");
                }
            }
        }
        Ok(())
    }
}
