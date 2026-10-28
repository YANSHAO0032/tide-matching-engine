//! 独立新资金预留的签名授权载荷。

use smallvec::SmallVec;

use crate::{AssetReserve, CommandId, MarketId, OrderId, RejectReason, ReservationId, UserId};

/// 账户风控为最终命令及新订单签发的独立预留授权。
///
/// 这里只定义规范载荷；真实验签、已登记身份冲突、费率及足额覆盖由资金层核验。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReservationToken {
    /// 授权令牌的稳定身份。
    pub token_id: u128,
    /// 授权前已分配的最终命令身份。
    pub command_id: CommandId,
    /// 独立新预留的身份。
    pub reservation_id: ReservationId,
    /// 被授权的新订单身份。
    pub order_id: OrderId,
    /// 被授权的用户身份。
    pub user_id: UserId,
    /// 被授权的市场身份。
    pub market_id: MarketId,
    /// 按资产严格升序的正预留原子数。
    pub reserves: SmallVec<[AssetReserve; 3]>,
    /// 接收时锁定的费率版本。
    pub fee_schedule_version: u64,
    /// 接收时锁定的费率层级。
    pub fee_tier_id: u32,
    /// 已加载风控公钥的标识符。
    pub risk_key_id: u32,
    /// 对规范载荷的 Ed25519 签名。
    pub signature: [u8; 64],
}

impl ReservationToken {
    /// 固定字段顺序及大端编码；资产须非空、严格升序且原子数为正。
    ///
    /// 签名自身不进入载荷；成功生成字节不表示签名、身份绑定或资产覆盖已经验证。
    pub fn canonical_signing_payload(&self) -> Result<Vec<u8>, RejectReason> {
        let reserve_count =
            u32::try_from(self.reserves.len()).map_err(|_| RejectReason::InvalidReservation)?;
        if reserve_count == 0 {
            return Err(RejectReason::InvalidReservation);
        }
        let mut previous_asset = None;
        for reserve in &self.reserves {
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
        payload.extend_from_slice(&reserve_count.to_be_bytes());
        for reserve in &self.reserves {
            payload.extend_from_slice(&reserve.asset.get().to_be_bytes());
            payload.extend_from_slice(&reserve.atoms.to_be_bytes());
        }
        payload.extend_from_slice(&self.fee_schedule_version.to_be_bytes());
        payload.extend_from_slice(&self.fee_tier_id.to_be_bytes());
        payload.extend_from_slice(&self.risk_key_id.to_be_bytes());
        Ok(payload)
    }
}
