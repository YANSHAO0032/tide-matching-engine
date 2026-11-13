//! 成交本金及锁定费率的只读结算预检。
use crate::{
    AssetScaleError, FeeCalculationError, PlannedR2, ReservationRegistry, ReservationState,
    ReservationStatus, SettlementDebit, calculate_fee, convert_asset_atoms,
};
use matching_domain::{
    AssetId, FeeAssetPolicy, FeeScheduleBook, MarketMath, MathError, RejectReason, ReservationId,
    Side, TradeEvent, TradeId, UserId,
};

/// 市场资产及其可信原子精度。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SettlementAsset {
    /// 资产身份。
    pub asset: AssetId,
    /// 账户使用的十进制原子精度。
    pub scale: u8,
}

/// 本次成交的可信资产、数值与费率配置。
pub struct TradeSettlementContext<'a> {
    /// 成交所属的可信市场身份。
    pub market_id: matching_domain::MarketId,
    /// 精确撮合本金规则。
    pub math: MarketMath,
    /// 基础资产及精度。
    pub base: SettlementAsset,
    /// 报价资产及精度。
    pub quote: SettlementAsset,
    /// 已加载的版本化费率簿。
    pub fees: &'a FeeScheduleBook,
}

/// 一方的有符号费用及锁定版本；负数为返佣。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeeAmount {
    /// 费用资产身份。
    pub asset: AssetId,
    /// 费用资产的有符号原子数量。
    pub atoms: i128,
    /// 接收时固定的费率版本。
    pub schedule_version: u64,
    /// 接收时固定的费率层级。
    pub tier_id: u32,
}

/// 单笔成交的只读结算预检结果；本金方向相对于挂单方。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TradeSettlement {
    /// 成交身份。
    pub trade_id: TradeId,
    /// 成交所属市场。
    pub market_id: matching_domain::MarketId,
    /// 成交基础资产。
    pub base_asset: AssetId,
    /// 成交报价资产。
    pub quote_asset: AssetId,
    /// 挂单方订单身份。
    pub maker_order: matching_domain::OrderId,
    /// 吃单方订单身份。
    pub taker_order: matching_domain::OrderId,
    /// 挂单方用户。
    pub maker_user: UserId,
    /// 吃单方用户。
    pub taker_user: UserId,
    /// 挂单方基础资产变动，吃单方为相反数。
    pub maker_base_delta: i128,
    /// 挂单方报价本金变动，吃单方为相反数。
    pub maker_quote_delta: i128,
    /// 挂单方费用。
    pub maker_fee: FeeAmount,
    /// 吃单方费用。
    pub taker_fee: FeeAmount,
    /// 挂单方预留身份。
    pub maker_reservation: ReservationId,
    /// 吃单方预留身份。
    pub taker_reservation: ReservationId,
}

impl TradeSettlement {
    /// 转换为独立的领域事件载荷；调用方仍须完成原子提交后才能发布。
    pub fn to_event_payload(&self) -> matching_domain::SettlementDelta {
        let fee = |value: FeeAmount| matching_domain::FeeDelta {
            asset: value.asset,
            amount: matching_domain::SignedAmount::new(value.atoms),
            fee_schedule_version: value.schedule_version,
            fee_tier_id: value.tier_id,
        };
        matching_domain::SettlementDelta {
            trade_id: self.trade_id,
            maker_user: self.maker_user,
            taker_user: self.taker_user,
            base_asset: self.base_asset,
            quote_asset: self.quote_asset,
            base_delta: matching_domain::SignedAmount::new(self.maker_base_delta),
            quote_delta: matching_domain::SignedAmount::new(self.maker_quote_delta),
            maker_fee: fee(self.maker_fee),
            taker_fee: fee(self.taker_fee),
            maker_reservation_id: self.maker_reservation,
            taker_reservation_id: self.taker_reservation,
        }
    }
}

/// 成交结算预检的内部错误分类。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SettlementPlanError {
    /// 订单、预留或市场身份不一致。
    InvalidReservation,
    /// 市场本金无法精确计算。
    Math(MathError),
    /// 资产原子单位无法精确转换。
    AssetScale(AssetScaleError),
    /// 锁定费率或层级不存在。
    FeeSchedule(RejectReason),
    /// 预留扣减或终结预检失败。
    Reservation(RejectReason),
    /// 第三资产费用尚缺可信计价映射。
    ThirdAssetBasisUnavailable,
    /// 本金或费用无法表示为有符号数量。
    Overflow,
}

/// 构造成交本金、费用和预留扣减的独立候选；调用方仍须原子安装簿与事件。
pub fn plan_trade_r2(
    context: TradeSettlementContext<'_>,
    trade: &TradeEvent,
    registry: &ReservationRegistry,
    maker_id: ReservationId,
    taker_id: ReservationId,
    terminal: &[ReservationId],
) -> Result<(TradeSettlement, PlannedR2), SettlementPlanError> {
    let maker = registry
        .get(maker_id)
        .ok_or(SettlementPlanError::InvalidReservation)?;
    let taker = registry
        .get(taker_id)
        .ok_or(SettlementPlanError::InvalidReservation)?;
    if terminal.iter().any(|id| *id != maker_id && *id != taker_id) {
        return Err(SettlementPlanError::InvalidReservation);
    }
    let settlement = plan_trade_settlement(context, trade, maker, taker)?;
    let mut debits = Vec::new();
    for (reservation_id, base_delta, quote_delta, fee) in [
        (
            maker_id,
            settlement.maker_base_delta,
            settlement.maker_quote_delta,
            settlement.maker_fee,
        ),
        (
            taker_id,
            settlement
                .maker_base_delta
                .checked_neg()
                .ok_or(SettlementPlanError::Overflow)?,
            settlement
                .maker_quote_delta
                .checked_neg()
                .ok_or(SettlementPlanError::Overflow)?,
            settlement.taker_fee,
        ),
    ] {
        for (asset, delta) in [
            (settlement.base_asset, base_delta),
            (settlement.quote_asset, quote_delta),
        ] {
            if delta < 0 {
                debits.push(SettlementDebit {
                    reservation_id,
                    asset,
                    atoms: delta.unsigned_abs(),
                });
            }
        }
        if fee.atoms > 0 {
            let positive = if fee.asset == settlement.base_asset && base_delta > 0 {
                base_delta.unsigned_abs()
            } else if fee.asset == settlement.quote_asset && quote_delta > 0 {
                quote_delta.unsigned_abs()
            } else {
                0
            };
            let fee_atoms = fee.atoms.unsigned_abs();
            if fee_atoms > positive {
                debits.push(SettlementDebit {
                    reservation_id,
                    asset: fee.asset,
                    atoms: fee_atoms
                        .checked_sub(positive)
                        .ok_or(SettlementPlanError::Overflow)?,
                });
            }
        }
    }
    let candidate = registry
        .plan_r2(&debits, terminal)
        .map_err(SettlementPlanError::Reservation)?;
    Ok((settlement, candidate))
}

/// 仅计算成交本金与双方费用，不修改订单簿、登记表或事件。
pub fn plan_trade_settlement(
    context: TradeSettlementContext<'_>,
    trade: &TradeEvent,
    maker: &ReservationState,
    taker: &ReservationState,
) -> Result<TradeSettlement, SettlementPlanError> {
    if maker.status != ReservationStatus::Active
        || taker.status != ReservationStatus::Active
        || maker.order_id != trade.maker_order_id
        || taker.order_id != trade.taker_order_id
        || maker.reservation_id == taker.reservation_id
        || maker.market_id != context.market_id
        || taker.market_id != context.market_id
        || context.base.asset == context.quote.asset
    {
        return Err(SettlementPlanError::InvalidReservation);
    }
    let base = convert_asset_atoms(
        u128::from(trade.qty.get()),
        context.math.qty_scale(),
        context.base.scale,
    )
    .map_err(SettlementPlanError::AssetScale)?;
    let quote_raw = context
        .math
        .trade_notional(trade.price, trade.qty)
        .map_err(SettlementPlanError::Math)?
        .get();
    let quote = convert_asset_atoms(quote_raw, context.math.quote_scale(), context.quote.scale)
        .map_err(SettlementPlanError::AssetScale)?;
    let base_signed = i128::try_from(base).map_err(|_| SettlementPlanError::Overflow)?;
    let quote_signed = i128::try_from(quote).map_err(|_| SettlementPlanError::Overflow)?;
    let fee =
        |state: &ReservationState, maker_side: bool| -> Result<FeeAmount, SettlementPlanError> {
            let schedule = context
                .fees
                .schedule(state.fee_schedule_version)
                .map_err(SettlementPlanError::FeeSchedule)?;
            let tier = context
                .fees
                .resolve(state.fee_schedule_version, state.fee_tier_id)
                .map_err(SettlementPlanError::FeeSchedule)?;
            let (asset, basis) = match schedule.asset_policy() {
                FeeAssetPolicy::Base => (context.base.asset, base),
                FeeAssetPolicy::Quote => (context.quote.asset, quote),
                FeeAssetPolicy::Third(_) => {
                    return Err(SettlementPlanError::ThirdAssetBasisUnavailable);
                }
                FeeAssetPolicy::ThirdPriced {
                    asset,
                    quote_asset,
                    numerator,
                    denominator,
                } => {
                    if asset == context.base.asset || quote_asset != context.quote.asset {
                        return Err(SettlementPlanError::InvalidReservation);
                    }
                    (
                        asset,
                        crate::asset_scale::convert_ratio_exact(quote, numerator, denominator)
                            .map_err(SettlementPlanError::AssetScale)?,
                    )
                }
            };
            let rate = if maker_side {
                tier.maker_bps
            } else {
                tier.taker_bps
            };
            let atoms = calculate_fee(basis, rate, tier.rounding)
                .map_err(|FeeCalculationError::Overflow| SettlementPlanError::Overflow)?;
            Ok(FeeAmount {
                asset,
                atoms,
                schedule_version: state.fee_schedule_version,
                tier_id: state.fee_tier_id,
            })
        };
    let (maker_base_delta, maker_quote_delta) = match trade.maker_side {
        Side::Buy => (base_signed, -quote_signed),
        Side::Sell => (-base_signed, quote_signed),
    };
    Ok(TradeSettlement {
        trade_id: trade.trade_id,
        market_id: context.market_id,
        base_asset: context.base.asset,
        quote_asset: context.quote.asset,
        maker_order: trade.maker_order_id,
        taker_order: trade.taker_order_id,
        maker_user: maker.user_id,
        taker_user: taker.user_id,
        maker_base_delta,
        maker_quote_delta,
        maker_fee: fee(maker, true)?,
        taker_fee: fee(taker, false)?,
        maker_reservation: maker.reservation_id,
        taker_reservation: taker.reservation_id,
    })
}
