//! 成交本金和双方费用的独立事件载荷。
use crate::{AssetId, ReservationId, SignedAmount, TradeId, UserId};

/// 一方费用的资产、金额及订单锁定的费率身份。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeeDelta {
    /// 费用资产。
    pub asset: AssetId,
    /// 正数为费用，负数为返佣。
    pub amount: SignedAmount,
    /// 该方订单锁定的费率版本。
    pub fee_schedule_version: u64,
    /// 该方订单锁定的费率层级。
    pub fee_tier_id: u32,
}

/// 单笔真实成交的本金和双方费用；本金符号相对挂单方。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SettlementDelta {
    /// 对应的真实成交身份。
    pub trade_id: TradeId,
    /// 挂单方用户。
    pub maker_user: UserId,
    /// 吃单方用户。
    pub taker_user: UserId,
    /// 基础资产身份。
    pub base_asset: AssetId,
    /// 报价资产身份。
    pub quote_asset: AssetId,
    /// 挂单方的基础资产变动，吃单方取相反数。
    pub base_delta: SignedAmount,
    /// 挂单方的报价本金变动，吃单方取相反数。
    pub quote_delta: SignedAmount,
    /// 挂单方的费用及锁定费率身份。
    pub maker_fee: FeeDelta,
    /// 吃单方的费用及锁定费率身份。
    pub taker_fee: FeeDelta,
    /// 挂单方的预留身份。
    pub maker_reservation_id: ReservationId,
    /// 吃单方的预留身份。
    pub taker_reservation_id: ReservationId,
}
