//! 已加载公钥对资金预留调整的本地验证与显式提交。

use std::collections::BTreeMap;

use ed25519_dalek::{Signature, VerifyingKey};
use matching_domain::{
    AssetReserve, CommandId, MarketId, OrderId, RejectReason, ReservationAdjustmentToken,
    ReservationId, UserId,
};

/// 配置状态中的公钥版本及按标识符确定性排序的已加载公钥。
#[derive(Clone, Debug)]
pub struct RiskKeySet {
    version: u64,
    keys: BTreeMap<u32, VerifyingKey>,
}

impl RiskKeySet {
    /// 从已加载公钥建立版本化索引，拒绝重复标识符。
    pub fn new(
        version: u64,
        keys: impl IntoIterator<Item = (u32, VerifyingKey)>,
    ) -> Result<Self, RejectReason> {
        let mut ordered = BTreeMap::new();
        for (id, key) in keys {
            if ordered.insert(id, key).is_some() {
                return Err(RejectReason::InvalidReservation);
            }
        }
        Ok(Self {
            version,
            keys: ordered,
        })
    }

    /// 读取配置版本，供持久化控制状态关联。
    pub const fn version(&self) -> u64 {
        self.version
    }
}

/// 当前订单对应的本地预留快照。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReservationState {
    pub reservation_id: ReservationId,
    pub order_id: OrderId,
    pub user_id: UserId,
    pub market_id: MarketId,
    pub version: u64,
    pub reserves: Vec<AssetReserve>,
}

/// 候选订单及命令要求的身份、费率和足额预留。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpectedAmendReservation {
    pub command_id: CommandId,
    pub reservation_id: ReservationId,
    pub order_id: OrderId,
    pub user_id: UserId,
    pub market_id: MarketId,
    pub fee_schedule_version: u64,
    pub fee_tier_id: u32,
    pub required_reserves: Vec<AssetReserve>,
}

/// 验签后可在成功提交边界应用的目标预留。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedReservationAdjustment {
    reservation_id: ReservationId,
    order_id: OrderId,
    user_id: UserId,
    market_id: MarketId,
    expected_version: u64,
    target_reserves: Vec<AssetReserve>,
}

/// 只读核对签名、身份、版本、费率和候选订单所需预留。
pub fn verify_amend_reservation(
    keys: &RiskKeySet,
    state: &ReservationState,
    expected: &ExpectedAmendReservation,
    token: &ReservationAdjustmentToken,
) -> Result<VerifiedReservationAdjustment, RejectReason> {
    let key = keys
        .keys
        .get(&token.risk_key_id)
        .ok_or(RejectReason::InvalidReservation)?;
    let payload = token.canonical_signing_payload()?;
    let signature = Signature::from_bytes(&token.signature);
    key.verify_strict(&payload, &signature)
        .map_err(|_| RejectReason::InvalidReservation)?;

    if token.command_id != expected.command_id
        || token.reservation_id != expected.reservation_id
        || token.order_id != expected.order_id
        || token.user_id != expected.user_id
        || token.market_id != expected.market_id
        || token.reservation_id != state.reservation_id
        || token.order_id != state.order_id
        || token.user_id != state.user_id
        || token.market_id != state.market_id
        || token.expected_reservation_version != state.version
        || token.fee_schedule_version != expected.fee_schedule_version
        || token.fee_tier_id != expected.fee_tier_id
    {
        return Err(RejectReason::InvalidReservation);
    }

    if expected.required_reserves.is_empty() {
        return Err(RejectReason::InvalidReservation);
    }
    let mut previous_asset = None;
    for required in &expected.required_reserves {
        if required.atoms == 0 || previous_asset.is_some_and(|asset| required.asset <= asset) {
            return Err(RejectReason::InvalidReservation);
        }
        previous_asset = Some(required.asset);
        let covered = token
            .target_reserves
            .binary_search_by_key(&required.asset, |reserve| reserve.asset)
            .is_ok_and(|index| token.target_reserves[index].atoms >= required.atoms);
        if !covered {
            return Err(RejectReason::InsufficientReservation);
        }
    }

    Ok(VerifiedReservationAdjustment {
        reservation_id: token.reservation_id,
        order_id: token.order_id,
        user_id: token.user_id,
        market_id: token.market_id,
        expected_version: token.expected_reservation_version,
        target_reserves: token.target_reserves.to_vec(),
    })
}

impl ReservationState {
    /// 在其他业务校验全部成功后，原子替换目标预留并推进一次版本。
    pub fn apply_verified_adjustment(
        &mut self,
        verified: &VerifiedReservationAdjustment,
    ) -> Result<(), RejectReason> {
        if self.reservation_id != verified.reservation_id
            || self.order_id != verified.order_id
            || self.user_id != verified.user_id
            || self.market_id != verified.market_id
            || self.version != verified.expected_version
        {
            return Err(RejectReason::InvalidReservation);
        }
        let next_version = self
            .version
            .checked_add(1)
            .ok_or(RejectReason::ArithmeticOverflow)?;
        self.reserves.clone_from(&verified.target_reserves);
        self.version = next_version;
        Ok(())
    }
}
