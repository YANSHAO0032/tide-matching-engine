//! 预留金融状态的规范快照编码。
use crate::{ReservationState, ReservationStatus};
use matching_domain::{
    AssetId, AssetReserve, MarketId, OrderId, OriginSessionId, ReservationId, UserId,
    snapshot_wire::{SnapshotDecoder, SnapshotEncoder, SnapshotWireError},
};

/// 编码完整预留状态，失败时不改变外层载荷。
pub fn encode_reservation_snapshot(
    writer: &mut SnapshotEncoder,
    state: &ReservationState,
) -> Result<(), SnapshotWireError> {
    if state.reserves.is_empty()
        || state.reserves.iter().any(|r| r.atoms == 0)
        || state.reserves.windows(2).any(|r| r[0].asset >= r[1].asset)
    {
        return Err(SnapshotWireError);
    }
    let mut w = SnapshotEncoder::new(256 * 1024 * 1024);
    w.u128(state.reservation_id.get())?;
    w.u128(state.order_id.get())?;
    w.u64(state.user_id.get())?;
    w.u32(state.market_id.0)?;
    w.u64(state.origin_session_id.0)?;
    w.u64(state.version)?;
    w.u32(u32::try_from(state.reserves.len()).map_err(|_| SnapshotWireError)?)?;
    for reserve in &state.reserves {
        w.u32(reserve.asset.0)?;
        w.u128(reserve.atoms)?;
    }
    w.u8(match state.status {
        ReservationStatus::Active => 0,
        ReservationStatus::Released => 1,
    })?;
    w.u64(state.fee_schedule_version)?;
    w.u32(state.fee_tier_id)?;
    writer.bytes(&w.finish())
}

/// 恢复预留状态并重新校验资产顺序与严格正原子数。
pub fn decode_reservation_snapshot(
    reader: &mut SnapshotDecoder<'_>,
) -> Result<ReservationState, SnapshotWireError> {
    let bytes = reader.bytes()?;
    let mut r = SnapshotDecoder::new(bytes, 256 * 1024 * 1024)?;
    let reservation_id = ReservationId::new(r.u128()?);
    let order_id = OrderId::new(r.u128()?);
    let user_id = UserId::new(r.u64()?);
    let market_id = MarketId(r.u32()?);
    let origin_session_id = OriginSessionId(r.u64()?);
    let version = r.u64()?;
    let count = usize::try_from(r.u32()?).map_err(|_| SnapshotWireError)?;
    if count == 0 || count > r.remaining() / 20 {
        return Err(SnapshotWireError);
    }
    let mut reserves = Vec::new();
    reserves.try_reserve(count).map_err(|_| SnapshotWireError)?;
    for _ in 0..count {
        let asset = AssetId(r.u32()?);
        let atoms = r.u128()?;
        if atoms == 0
            || reserves
                .last()
                .is_some_and(|previous: &AssetReserve| previous.asset >= asset)
        {
            return Err(SnapshotWireError);
        }
        reserves.push(AssetReserve { asset, atoms });
    }
    let status = match r.u8()? {
        0 => ReservationStatus::Active,
        1 => ReservationStatus::Released,
        _ => return Err(SnapshotWireError),
    };
    let fee_schedule_version = r.u64()?;
    let fee_tier_id = r.u32()?;
    r.finish()?;
    Ok(ReservationState {
        reservation_id,
        order_id,
        user_id,
        market_id,
        origin_session_id,
        version,
        reserves,
        status,
        fee_schedule_version,
        fee_tier_id,
    })
}
