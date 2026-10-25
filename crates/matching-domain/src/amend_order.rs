//! 改单绝对目标载荷、字段模式校验及普通挂单候选计算。

use crate::{
    MarketId, MarketProtection, OrderId, Price, PriceReferenceKind, Qty, QuoteAmount, RejectReason,
    ReservationAdjustmentToken, TimeInForce, TriggerExpiry, UserId,
};

/// 未触发止损订单的存活期修改，明确区分保留与清除。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TriggerExpiryPatch {
    /// 保留当前存活期。
    Keep,
    /// 清除到期约束，改为持续有效。
    ClearToGtc,
    /// 设置新的存活期。
    Set(TriggerExpiry),
}

/// 市价保护修改，明确区分保留与清除。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MarketProtectionPatch {
    /// 保留当前保护参数。
    Keep,
    /// 清除当前保护参数。
    Clear,
    /// 设置新的保护参数。
    Set(MarketProtection),
}

/// 改单命令的绝对目标字段；未给定的数量目标保持原始总量语义。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AmendOrder {
    /// 订单所属市场。
    pub market_id: MarketId,
    /// 订单所属用户。
    pub user_id: UserId,
    /// 保持不变的订单身份。
    pub order_id: OrderId,
    /// 可选的新限价。
    pub new_price: Option<Price>,
    /// 可选的新基础资产原始总量，包含已成交量。
    pub new_qty: Option<Qty>,
    /// 可选的新报价资产预算目标；校验时要求严格正值。
    pub new_quote_budget: Option<QuoteAmount>,
    /// 可选的新冰山显示上限。
    pub new_visible_qty: Option<Qty>,
    /// 可选的新止损触发价。
    pub new_stop_price: Option<Price>,
    /// 可选的新止损触发参考来源。
    pub new_trigger_reference: Option<PriceReferenceKind>,
    /// 可选的未触发止损订单存活期补丁。
    pub new_trigger_expiry: Option<TriggerExpiryPatch>,
    /// 可选的新订单有效期。
    pub new_tif: Option<TimeInForce>,
    /// 可选的市价保护补丁。
    pub new_market_protection: Option<MarketProtectionPatch>,
    /// 可选的预留调整授权。
    pub reservation_adjustment: Option<ReservationAdjustmentToken>,
}

/// 当前订单的数量计价模式，由调用方根据已存在订单确定。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AmendQuantityMode {
    /// 基础资产总量模式。
    BaseQty,
    /// 尚未触发的报价预算止损市价模式。
    UntriggeredStopMarketByQuote,
}

/// 检查改单数量字段是否与现有订单模式一致。
///
/// 这里不查询运行中订单类型；调用方负责提供已核实的模式。
pub fn validate_amend_quantity_mode(
    mode: AmendQuantityMode,
    new_qty: Option<Qty>,
    new_quote_budget: Option<QuoteAmount>,
) -> Result<(), RejectReason> {
    match mode {
        AmendQuantityMode::BaseQty if new_quote_budget.is_some() => {
            Err(RejectReason::InvalidOrderCombination)
        }
        AmendQuantityMode::UntriggeredStopMarketByQuote if new_qty.is_some() => {
            Err(RejectReason::InvalidOrderCombination)
        }
        _ if new_quote_budget.is_some_and(|amount| amount.get() == 0) => {
            Err(RejectReason::InvalidQty)
        }
        _ => Ok(()),
    }
}

/// 根据原始总量和当前余量，计算新绝对总量对应的严格正余量。
///
/// 未给定目标时仍验证原始总量不小于当前余量，然后保留当前余量。
pub fn remaining_after_target_total(
    original_total: Qty,
    current_remaining: Qty,
    new_total: Option<Qty>,
) -> Result<Qty, RejectReason> {
    let filled = original_total
        .get()
        .checked_sub(current_remaining.get())
        .ok_or(RejectReason::ArithmeticOverflow)?;
    let target = new_total.unwrap_or(original_total);
    let remaining = target
        .get()
        .checked_sub(filled)
        .ok_or(RejectReason::InvalidQty)?;
    Qty::try_new(remaining)
}

/// 普通挂单修改后的队列及撮合路径分类。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegularAmendKind {
    /// 同价减量或数量不变，保留原队列位置。
    InPlaceReduce,
    /// 同价增量，重新排至档位尾部。
    RequeueSamePrice,
    /// 改价后按新价格重新尝试撮合。
    RepriceAndMatch,
}

/// 按价格与改单前后余量决定普通挂单路径，不读取或修改订单簿。
pub fn classify_regular_amend(
    old_price: Price,
    new_price: Price,
    old_remaining: Qty,
    new_remaining: Qty,
) -> RegularAmendKind {
    if new_price != old_price {
        RegularAmendKind::RepriceAndMatch
    } else if new_remaining > old_remaining {
        RegularAmendKind::RequeueSamePrice
    } else {
        RegularAmendKind::InPlaceReduce
    }
}

/// 改单事件中互不混淆的候选数量模式。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AmendedQuantity {
    /// 改单时、再次撮合前的基础资产余量。
    BaseRemaining(Qty),
    /// 尚未触发的止损市价订单的报价预算。
    QuoteBudget(QuoteAmount),
}
