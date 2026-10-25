//! 调用方固定的改单数值与资金核验上下文，以及本地只读预留计划。
use matching_domain::{AmendOrder, AssetReserve, CommandId, RejectReason};
use matching_risk::{
    ExpectedAmendReservation, ReservationState, RiskKeySet, verify_amend_reservation,
};

/// 已固定市场数值规则、命令身份、费率及候选订单资金上界的执行上下文。
///
/// 调用方必须使用本次候选订单及实际版本化市场、资产和费率配置，计算包含本金、
/// 最大费用与取整安全量的完整资产预留列表。该列表严格按资产升序且不可为空。
/// 当前簿不拥有资产映射或费率表，因此本类型明确保留此信任边界；不得从令牌自身
/// 复制所需预留，不得给未知配置填默认值。公钥必须在调用前完成加载。
pub struct AmendExecutionContext<'a> {
    /// 本命令开始固定的实际市场数值规则，不使用默认价格或数量步长。
    pub market_math: matching_domain::MarketMath,
    /// 与调整令牌绑定、在提交命令前已确定的身份。
    pub command_id: CommandId,
    /// 本次候选校验固定使用的费率版本；未触发止损必须匹配接收时固定的版本。
    pub fee_schedule_version: u64,
    /// 本次用户实际适用的费率层级。
    pub fee_tier_id: u32,
    /// 已加载的版本化风险公钥。
    pub risk_keys: &'a RiskKeySet,
    /// 基于候选新订单和真实配置计算的完整资金上界。
    pub required_reserves: Vec<AssetReserve>,
}

impl AmendExecutionContext<'_> {
    /// 价格构造器只保证正值，此处使用本命令固定的实际市场步长。
    pub(crate) fn validate_price(&self, price: matching_domain::Price) -> Result<(), RejectReason> {
        if price.get() % self.market_math.tick_size() != 0 {
            return Err(RejectReason::InvalidTick);
        }
        Ok(())
    }

    /// 基础数量与显示峰值都必须对齐实际市场数量步长。
    pub(crate) fn validate_qty(&self, qty: matching_domain::Qty) -> Result<(), RejectReason> {
        if !qty.get().is_multiple_of(self.market_math.lot_size()) {
            return Err(RejectReason::InvalidLot);
        }
        Ok(())
    }

    /// 验签与版本递增只作用于可丢弃副本；真实状态在全部簿预检后才替换。
    pub(crate) fn prepare(
        &self,
        amend: &AmendOrder,
        state: &ReservationState,
    ) -> Result<ReservationState, RejectReason> {
        if state.order_id != amend.order_id
            || state.user_id != amend.user_id
            || state.market_id != amend.market_id
            || self.required_reserves.is_empty()
        {
            return Err(RejectReason::InvalidReservation);
        }
        let mut previous = None;
        for reserve in &self.required_reserves {
            if reserve.atoms == 0 || previous.is_some_and(|asset| reserve.asset <= asset) {
                return Err(RejectReason::InvalidReservation);
            }
            previous = Some(reserve.asset);
        }
        let mut target = state.clone();
        if let Some(token) = &amend.reservation_adjustment {
            let verified = verify_amend_reservation(
                self.risk_keys,
                state,
                &ExpectedAmendReservation {
                    command_id: self.command_id,
                    reservation_id: state.reservation_id,
                    order_id: amend.order_id,
                    user_id: amend.user_id,
                    market_id: amend.market_id,
                    fee_schedule_version: self.fee_schedule_version,
                    fee_tier_id: self.fee_tier_id,
                    required_reserves: self.required_reserves.clone(),
                },
                token,
            )?;
            target.apply_verified_adjustment(&verified)?;
        } else {
            let mut previous = None;
            for reserve in &state.reserves {
                if reserve.atoms == 0 || previous.is_some_and(|asset| reserve.asset <= asset) {
                    return Err(RejectReason::InvalidReservation);
                }
                previous = Some(reserve.asset);
            }
            for reserve in &self.required_reserves {
                if !state
                    .reserves
                    .iter()
                    .any(|covered| covered.asset == reserve.asset && covered.atoms >= reserve.atoms)
                {
                    return Err(RejectReason::InsufficientReservation);
                }
            }
        }
        Ok(target)
    }
}
