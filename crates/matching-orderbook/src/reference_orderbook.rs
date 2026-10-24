//! 独立 Vec 参考订单簿的状态、构造与基础只读视图。
//! 挂单、逐笔撮合、限价策略、市价和冰山生命周期在各子模块实现。

use self::stop_orders::ReferenceStopOrder;
use crate::ReferenceOrder;
use matching_domain::{MathError, PriceReferenceState, RejectReason, Side};

mod expiry;
mod iceberg;
mod limit_execution;
mod market_by_base;
mod market_by_quote;
mod matching;
mod resting;
mod stop_orders;
mod stop_trigger;

/// 为优先级计数器保留不可分配的高位保护空间。
///
/// 到达该边界前必须拒绝新挂单订单，避免计数器回绕后破坏先进先出语义。
const PRIORITY_GUARD: u128 = 1_024;

/// 单市场参考订单簿
///
/// 订单集合的原始顺序表示已知挂单先后。
#[derive(Debug, Default)]
pub struct ReferenceOrderBook {
    /// 挂单的权威存储顺序；同价先进先出的参考基线由该顺序加稳定排序得出。
    orders: Vec<ReferenceOrder>,
    /// 下一次实际挂单或冰山补量将取得的市场内优先级。
    next_priority: u128,
    /// 未触发止损订单的独立参考存储；源 Vec 保留同价登记顺序。
    stop_orders: Vec<ReferenceStopOrder>,
    /// 下一次未触发止损订单将取得的独立 StopPriority。
    next_stop_priority: u128,
    /// 各参考来源的最近已接受价格和序号。
    reference_prices: PriceReferenceState,
}

impl ReferenceOrderBook {
    /// 创建不含挂单且优先级从零开始的单市场参考订单簿。
    pub fn new() -> Self {
        Self::default()
    }

    /// 返回指定侧按价格优先、同价先进先出排列的只读视图
    ///
    /// 买入: 价格降序
    /// 卖出: 价格升序
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

    /// 返回指定侧价格/先进先出优先级最高的订单。
    ///
    pub fn best(&self, side: Side) -> Option<&ReferenceOrder> {
        self.orders(side).into_iter().next()
    }

    fn reject_quote_math_error(error: MathError) -> RejectReason {
        error.to_reject_reason().unwrap_or_else(|| {
            panic!(
                "ReferenceOrderBook invariant violation: \
             non-rejectable MarketByQuote math error: {error:?}"
            )
        })
    }
}

#[cfg(test)]
mod tests;
