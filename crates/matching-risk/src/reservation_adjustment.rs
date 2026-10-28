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
    /// 已加载公钥集合对应的持久化配置版本。
    version: u64,
    /// 按稳定公钥标识符索引的验签公钥。
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

    /// 仅接受当前已加载公钥，对规范载荷执行严格签名验证。
    pub(crate) fn verify_signature(
        &self,
        key_id: u32,
        payload: &[u8],
        signature: &[u8; 64],
    ) -> Result<(), RejectReason> {
        let key = self
            .keys
            .get(&key_id)
            .ok_or(RejectReason::InvalidReservation)?;
        key.verify_strict(payload, &Signature::from_bytes(signature))
            .map_err(|_| RejectReason::InvalidReservation)
    }
}

/// 当前订单对应的本地预留快照。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReservationState {
    /// 预留的全局唯一身份；即使释放后也不得重新接管。
    pub reservation_id: ReservationId,
    /// 当前或最近绑定订单的身份。
    pub order_id: OrderId,
    /// 预留所属用户身份。
    pub user_id: UserId,
    /// 预留生效的市场身份。
    pub market_id: MarketId,
    /// 用于拒绝陈旧授权的单调版本。
    pub version: u64,
    /// 按资产严格升序保存的已锁定原子数量。
    pub reserves: Vec<AssetReserve>,
    /// 当前预留是否仍被引擎接管。
    pub status: ReservationStatus,
    /// 接管时锁定的费率版本。
    pub fee_schedule_version: u64,
    /// 接管时锁定的费率层级。
    pub fee_tier_id: u32,
}

/// 引擎本地预留的接管状态；释放历史继续占用预留身份。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReservationStatus {
    /// 金融预留仍被接管，归属可以是活动订单或等待后置资金处理的 PendingR2。
    Active,
    /// 已解除接管，不得再次应用调整或重新登记。
    Released,
}

/// 候选订单及命令要求的身份、费率和足额预留。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpectedAmendReservation {
    /// 本次调整预先分配的最终命令身份。
    pub command_id: CommandId,
    /// 必须与当前快照一致的预留身份。
    pub reservation_id: ReservationId,
    /// 必须与当前快照一致的活动订单身份。
    pub order_id: OrderId,
    /// 必须与当前快照一致的用户身份。
    pub user_id: UserId,
    /// 必须与当前快照一致的市场身份。
    pub market_id: MarketId,
    /// 调整前后均须匹配的锁定费率版本。
    pub fee_schedule_version: u64,
    /// 调整前后均须匹配的锁定费率层级。
    pub fee_tier_id: u32,
    /// 候选订单所需的最小资产预留，按资产严格升序。
    pub required_reserves: Vec<AssetReserve>,
}

/// 验签后可在成功提交边界应用的目标预留。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedReservationAdjustment {
    /// 已验证的预留身份。
    reservation_id: ReservationId,
    /// 授权绑定的活动订单身份。
    order_id: OrderId,
    /// 授权绑定的用户身份。
    user_id: UserId,
    /// 授权绑定的市场身份。
    market_id: MarketId,
    /// 提交时必须仍匹配的旧版本。
    expected_version: u64,
    /// 提交时必须仍匹配的锁定费率版本。
    fee_schedule_version: u64,
    /// 提交时必须仍匹配的锁定费率层级。
    fee_tier_id: u32,
    /// 已验签且足额覆盖的目标资产预留。
    target_reserves: Vec<AssetReserve>,
}

/// 只读核对签名、身份、版本、费率和候选订单所需预留。
pub fn verify_amend_reservation(
    keys: &RiskKeySet,
    state: &ReservationState,
    expected: &ExpectedAmendReservation,
    token: &ReservationAdjustmentToken,
) -> Result<VerifiedReservationAdjustment, RejectReason> {
    let payload = token.canonical_signing_payload()?;
    keys.verify_signature(token.risk_key_id, &payload, &token.signature)?;

    if state.status != ReservationStatus::Active
        || state.fee_schedule_version != expected.fee_schedule_version
        || state.fee_tier_id != expected.fee_tier_id
        || token.command_id != expected.command_id
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
        fee_schedule_version: token.fee_schedule_version,
        fee_tier_id: token.fee_tier_id,
        target_reserves: token.target_reserves.to_vec(),
    })
}

impl ReservationState {
    /// 在其他业务校验全部成功后，原子替换目标预留并推进一次版本。
    pub fn apply_verified_adjustment(
        &mut self,
        verified: &VerifiedReservationAdjustment,
    ) -> Result<(), RejectReason> {
        if self.status != ReservationStatus::Active
            || self.fee_schedule_version != verified.fee_schedule_version
            || self.fee_tier_id != verified.fee_tier_id
            || self.reservation_id != verified.reservation_id
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
