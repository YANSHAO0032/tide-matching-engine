//! 资金预留调整的绝对目标与规范签名载荷。

use smallvec::SmallVec;

use crate::{AssetId, CommandId, MarketId, OrderId, RejectReason, ReservationId, UserId};

/// 单一资产的目标预留原子数。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AssetReserve {
    pub asset: AssetId,
    pub atoms: u128,
}

/// 账户风控在命令身份确定后签发的绝对目标预留授权。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReservationAdjustmentToken {
    pub token_id: u128,
    pub command_id: CommandId,
    pub reservation_id: ReservationId,
    pub order_id: OrderId,
    pub user_id: UserId,
    pub market_id: MarketId,
    pub expected_reservation_version: u64,
    pub target_reserves: SmallVec<[AssetReserve; 3]>,
    pub fee_schedule_version: u64,
    pub fee_tier_id: u32,
    pub risk_key_id: u32,
    pub signature: [u8; 64],
}

impl ReservationAdjustmentToken {
    /// 按固定字段顺序生成大端签名载荷，不包含签名本身。
    pub fn canonical_signing_payload(&self) -> Result<Vec<u8>, RejectReason> {
        let reserve_count = u32::try_from(self.target_reserves.len())
            .map_err(|_| RejectReason::InvalidReservation)?;
        if reserve_count == 0 {
            return Err(RejectReason::InvalidReservation);
        }
        let mut previous_asset = None;
        for reserve in &self.target_reserves {
            if reserve.atoms == 0 || previous_asset.is_some_and(|asset| reserve.asset <= asset) {
                return Err(RejectReason::InvalidReservation);
            }
            previous_asset = Some(reserve.asset);
        }

        let mut payload = Vec::new();
        payload.extend_from_slice(&self.token_id.to_be_bytes());
        payload.extend_from_slice(&self.command_id.get().to_be_bytes());
        payload.extend_from_slice(&self.reservation_id.get().to_be_bytes());
        payload.extend_from_slice(&self.order_id.get().to_be_bytes());
        payload.extend_from_slice(&self.user_id.get().to_be_bytes());
        payload.extend_from_slice(&self.market_id.get().to_be_bytes());
        payload.extend_from_slice(&self.expected_reservation_version.to_be_bytes());
        payload.extend_from_slice(&reserve_count.to_be_bytes());
        for reserve in &self.target_reserves {
            payload.extend_from_slice(&reserve.asset.get().to_be_bytes());
            payload.extend_from_slice(&reserve.atoms.to_be_bytes());
        }
        payload.extend_from_slice(&self.fee_schedule_version.to_be_bytes());
        payload.extend_from_slice(&self.fee_tier_id.to_be_bytes());
        payload.extend_from_slice(&self.risk_key_id.to_be_bytes());
        Ok(payload)
    }
}
