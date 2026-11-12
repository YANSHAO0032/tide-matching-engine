//! 用于确定性事件摘要的版本化规范编码。
use crate::{
    AmendedQuantity, EngineEvent, EventEnvelope, OrderRemaining, Price, Side,
    snapshot_wire::{SnapshotEncoder, SnapshotWireError},
};

/// 将事件身份和完整载荷写为带长度前缀的大端编码；失败不改变外层编码器。
///
/// 此格式用于摘要比较，不是对外事件发布协议。
pub fn encode_event_for_hash(
    writer: &mut SnapshotEncoder,
    event: &EventEnvelope,
) -> Result<(), SnapshotWireError> {
    let mut w = SnapshotEncoder::new(1024);
    w.u8(1)?;
    w.u16(event.cluster_id.get())?;
    w.u16(event.shard_id.get())?;
    w.u64(event.shard_seq)?;
    w.u32(event.event_index)?;
    w.u128(event.command_id.get())?;
    w.u64(event.engine_time)?;
    match event.market_id {
        None => w.u8(0)?,
        Some(id) => {
            w.u8(1)?;
            w.u32(id.get())?;
        }
    }
    match &event.payload {
        EngineEvent::OrderExpired { order_id, reason } => {
            w.u8(19)?;
            w.u128(order_id.get())?;
            w.u16(*reason as u16)?;
        }
        EngineEvent::MarketCreated { market_id } => {
            w.u8(15)?;
            w.u32(market_id.get())?;
        }
        EngineEvent::MarketConfigUpdated { market_id } => {
            w.u8(16)?;
            w.u32(market_id.get())?;
        }
        EngineEvent::SessionStarted {
            market_id,
            session_id,
        } => {
            w.u8(17)?;
            w.u32(market_id.get())?;
            w.u64(*session_id)?;
        }
        EngineEvent::SessionEnded {
            market_id,
            session_id,
        } => {
            w.u8(18)?;
            w.u32(market_id.get())?;
            w.u64(*session_id)?;
        }
        EngineEvent::MarketStateChanged {
            market_id,
            old,
            new,
            reason,
        } => {
            w.u8(14)?;
            w.u32(market_id.get())?;
            let tag = |state| match state {
                crate::MarketState::PreOpen => 0,
                crate::MarketState::Open => 1,
                crate::MarketState::Halted => 2,
                crate::MarketState::Suspended => 3,
                crate::MarketState::Closed => 4,
            };
            w.u8(tag(*old))?;
            w.u8(tag(*new))?;
            w.u16(*reason as u16)?;
        }
        EngineEvent::UserControlEpochAdvanced { user_id, epoch } => {
            w.u8(0)?;
            w.u64(user_id.get())?;
            w.u64(*epoch)?;
        }
        EngineEvent::CommandRejected { reason } => {
            w.u8(1)?;
            w.u16(*reason as u16)?;
        }
        EngineEvent::OrderAccepted { order_id } => {
            w.u8(2)?;
            w.u128(order_id.get())?;
        }
        EngineEvent::OrderOpened { order_id } => {
            w.u8(3)?;
            w.u128(order_id.get())?;
        }
        EngineEvent::OrderAmended {
            order_id,
            old_price,
            new_price,
            old_quantity,
            new_quantity,
            priority_retained,
        } => {
            w.u8(4)?;
            w.u128(order_id.get())?;
            price(&mut w, *old_price)?;
            price(&mut w, *new_price)?;
            amended(&mut w, *old_quantity)?;
            amended(&mut w, *new_quantity)?;
            w.u8(u8::from(*priority_retained))?;
        }
        EngineEvent::OrderPartiallyFilled {
            order_id,
            remaining,
        } => {
            w.u8(5)?;
            w.u128(order_id.get())?;
            w.u64(remaining.get())?;
        }
        EngineEvent::OrderFilled { order_id } => {
            w.u8(6)?;
            w.u128(order_id.get())?;
        }
        EngineEvent::OrderCanceled {
            order_id,
            remaining,
            reason,
        } => {
            w.u8(7)?;
            w.u128(order_id.get())?;
            match remaining {
                OrderRemaining::Base(q) => {
                    w.u8(0)?;
                    w.u64(q.get())?;
                }
                OrderRemaining::Quote(q) => {
                    w.u8(1)?;
                    w.u128(q.get())?;
                }
            }
            w.u16(*reason as u16)?;
        }
        EngineEvent::StopTriggered {
            order_id,
            child_kind,
        } => {
            w.u8(8)?;
            w.u128(order_id.get())?;
            w.u8(*child_kind as u8)?;
        }
        EngineEvent::TriggeredOrderRejected { order_id, reason } => {
            w.u8(9)?;
            w.u128(order_id.get())?;
            w.u16(*reason as u16)?;
        }
        EngineEvent::ReservationReleased {
            order_id,
            reservation_id,
            reason,
        } => {
            w.u8(10)?;
            w.u128(order_id.get())?;
            w.u128(reservation_id.get())?;
            w.u16(*reason as u16)?;
        }
        EngineEvent::Trade(trade) => {
            w.u8(11)?;
            w.u128(trade.trade_id.get())?;
            w.u128(trade.maker_order_id.get())?;
            w.u128(trade.taker_order_id.get())?;
            w.i64(trade.price.get())?;
            w.u64(trade.qty.get())?;
            w.u8(match trade.maker_side {
                Side::Buy => 0,
                Side::Sell => 1,
            })?;
        }
        EngineEvent::IcebergRefreshed { order_id, visible } => {
            w.u8(12)?;
            w.u128(order_id.get())?;
            w.u64(visible.get())?;
        }
        EngineEvent::MarketProtectionTriggered {
            order_id,
            next_price,
            effective_worst_price,
            source,
        } => {
            w.u8(13)?;
            w.u128(order_id.get())?;
            w.i64(next_price.get())?;
            w.i64(effective_worst_price.get())?;
            w.u8(*source as u8)?;
        }
    }
    writer.bytes(&w.finish())
}
/// 编码可选价格及其存在标记。
fn price(w: &mut SnapshotEncoder, p: Option<Price>) -> Result<(), SnapshotWireError> {
    match p {
        None => w.u8(0),
        Some(p) => {
            w.u8(1)?;
            w.i64(p.get())
        }
    }
}
/// 编码改单数量的计价类型与值。
fn amended(w: &mut SnapshotEncoder, q: AmendedQuantity) -> Result<(), SnapshotWireError> {
    match q {
        AmendedQuantity::BaseRemaining(q) => {
            w.u8(0)?;
            w.u64(q.get())
        }
        AmendedQuantity::QuoteBudget(q) => {
            w.u8(1)?;
            w.u128(q.get())
        }
    }
}
