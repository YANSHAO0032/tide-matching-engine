//! 生产订单簿的确定性快照与恢复校验。
use super::{ProductionOrderBook, RegularExpiryIndex};
use crate::{
    OrderArena, OrderIndex, PriceLevel, QueuePriorityAllocator, StopBook, StopPriorityAllocator,
};
use matching_domain::{
    MarketId, Price, PriceBandConfig, PriceReferenceKind,
    snapshot_wire::{
        SnapshotDecoder, SnapshotEncoder, SnapshotWireError, decode_market_math,
        decode_reference_state, encode_market_math, encode_reference_state,
    },
};
use std::collections::{BTreeMap, HashMap};

impl ProductionOrderBook {
    /// 按业务价序和链表顺序编码状态，不包含 Arena 物理槽位。
    pub fn write_canonical_state(
        &self,
        writer: &mut SnapshotEncoder,
    ) -> Result<(), SnapshotWireError> {
        self.validate_snapshot_invariants()
            .map_err(|_| SnapshotWireError)?;
        let mut w = SnapshotEncoder::new(256 * 1024 * 1024);
        match self.market_id {
            None => w.u8(0)?,
            Some(id) => {
                w.u8(1)?;
                w.u32(id.get())?;
            }
        }
        encode_reference_state(&mut w, &self.reference_prices)?;
        match self.price_band {
            None => w.u8(0)?,
            Some((band, math)) => {
                w.u8(1)?;
                w.u8(band.reference_kind as u8)?;
                w.u32(band.max_deviation_bps)?;
                encode_market_math(&mut w, &math)?;
            }
        }
        for (reverse, levels) in [(true, &self.bids), (false, &self.asks)] {
            w.u32(u32::try_from(levels.len()).map_err(|_| SnapshotWireError)?)?;
            let ordered: Box<dyn Iterator<Item = (&Price, &PriceLevel)>> = if reverse {
                Box::new(levels.iter().rev())
            } else {
                Box::new(levels.iter())
            };
            for (price, level) in ordered {
                w.i64(price.get())?;
                w.u32(level.count())?;
                let mut index = level.head();
                while let Some(current) = index {
                    let node = self.arena.get(current).ok_or(SnapshotWireError)?;
                    index = node.next();
                    let mut value = node.clone();
                    value.set_prev(None);
                    value.set_next(None);
                    value.write_snapshot(&mut w)?;
                }
            }
        }
        self.stop_book.write_canonical_state(&mut w)?;
        self.queue_priority_allocator.write_snapshot(&mut w)?;
        self.stop_priority_allocator.write_snapshot(&mut w)?;
        let reservations = self.reservation_snapshots();
        w.u32(u32::try_from(reservations.len()).map_err(|_| SnapshotWireError)?)?;
        for (_, state) in reservations {
            matching_risk::encode_reservation_snapshot(&mut w, state)?;
        }
        writer.bytes(&w.finish())
    }
    /// 编码完整本地簿状态；位置与到期索引在恢复时重建。
    pub fn write_snapshot(&self, writer: &mut SnapshotEncoder) -> Result<(), SnapshotWireError> {
        self.validate_snapshot_invariants()
            .map_err(|_| SnapshotWireError)?;
        let mut w = SnapshotEncoder::new(256 * 1024 * 1024);
        match self.market_id {
            None => w.u8(0)?,
            Some(id) => {
                w.u8(1)?;
                w.u32(id.0)?
            }
        }
        self.arena.write_snapshot(&mut w)?;
        for levels in [&self.bids, &self.asks] {
            w.u32(u32::try_from(levels.len()).map_err(|_| SnapshotWireError)?)?;
            for (price, level) in levels {
                w.i64(price.get())?;
                level.write_snapshot(&mut w)?;
            }
        }
        self.queue_priority_allocator.write_snapshot(&mut w)?;
        self.stop_book.write_snapshot(&mut w)?;
        self.stop_priority_allocator.write_snapshot(&mut w)?;
        encode_reference_state(&mut w, &self.reference_prices)?;
        match self.price_band {
            None => w.u8(0)?,
            Some((band, math)) => {
                if band.max_deviation_bps >= 10_000 {
                    return Err(SnapshotWireError);
                }
                w.u8(1)?;
                w.u8(band.reference_kind as u8)?;
                w.u32(band.max_deviation_bps)?;
                encode_market_math(&mut w, &math)?;
            }
        }
        let states = self.reservation_snapshots();
        w.u32(u32::try_from(states.len()).map_err(|_| SnapshotWireError)?)?;
        for (id, state) in states {
            if id != state.order_id {
                return Err(SnapshotWireError);
            }
            matching_risk::encode_reservation_snapshot(&mut w, state)?;
        }
        writer.bytes(&w.finish())
    }

    /// 从完整编码重建订单簿，拒绝任何结构或派生索引不一致。
    pub fn read_snapshot(reader: &mut SnapshotDecoder<'_>) -> Result<Self, SnapshotWireError> {
        let bytes = reader.bytes()?;
        let mut r = SnapshotDecoder::new(bytes, 256 * 1024 * 1024)?;
        let market_id = match r.u8()? {
            0 => None,
            1 => Some(MarketId(r.u32()?)),
            _ => return Err(SnapshotWireError),
        };
        let arena = OrderArena::read_snapshot(&mut r)?;
        let mut sides = [BTreeMap::new(), BTreeMap::new()];
        for levels in &mut sides {
            let count = usize::try_from(r.u32()?).map_err(|_| SnapshotWireError)?;
            if count > r.remaining() / 12 {
                return Err(SnapshotWireError);
            }
            let mut previous = None;
            for _ in 0..count {
                let price = Price::try_new(r.i64()?).map_err(|_| SnapshotWireError)?;
                if previous.is_some_and(|p| p >= price) {
                    return Err(SnapshotWireError);
                }
                previous = Some(price);
                let level = PriceLevel::read_snapshot(&mut r)?;
                levels.insert(price, level);
            }
        }
        let queue_priority_allocator = QueuePriorityAllocator::read_snapshot(&mut r)?;
        let stop_book = StopBook::read_snapshot(&mut r)?;
        let stop_priority_allocator = StopPriorityAllocator::read_snapshot(&mut r)?;
        let reference_prices = decode_reference_state(&mut r)?;
        let price_band = match r.u8()? {
            0 => None,
            1 => {
                let reference_kind = match r.u8()? {
                    1 => PriceReferenceKind::LastTrade,
                    2 => PriceReferenceKind::MarkPrice,
                    3 => PriceReferenceKind::IndexPrice,
                    4 => PriceReferenceKind::StaticOpening,
                    _ => return Err(SnapshotWireError),
                };
                let max_deviation_bps = r.u32()?;
                if max_deviation_bps >= 10_000 {
                    return Err(SnapshotWireError);
                }
                Some((
                    PriceBandConfig {
                        reference_kind,
                        max_deviation_bps,
                    },
                    decode_market_math(&mut r)?,
                ))
            }
            _ => return Err(SnapshotWireError),
        };
        let count = usize::try_from(r.u32()?).map_err(|_| SnapshotWireError)?;
        if count > r.remaining() / 4 {
            return Err(SnapshotWireError);
        }
        let mut reservations = HashMap::new();
        reservations
            .try_reserve(count)
            .map_err(|_| SnapshotWireError)?;
        let mut previous = None;
        for _ in 0..count {
            let state = matching_risk::decode_reservation_snapshot(&mut r)?;
            let id = state.order_id;
            if previous.is_some_and(|p| p >= id) {
                return Err(SnapshotWireError);
            }
            previous = Some(id);
            reservations.insert(id, state);
        }
        r.finish()?;
        let mut orders = HashMap::new();
        let mut regular_expiry = RegularExpiryIndex::default();
        let (slots, _) = arena.state_snapshot();
        orders
            .try_reserve(slots.len())
            .map_err(|_| SnapshotWireError)?;
        for (index, node) in slots.iter().enumerate() {
            if let Some(node) = node {
                node.validate_snapshot_fields()?;
                let id = node.original_order().order_id;
                let idx = OrderIndex::try_from_usize(index).map_err(|_| SnapshotWireError)?;
                if orders.insert(id, idx).is_some() {
                    return Err(SnapshotWireError);
                }
                regular_expiry.insert(id, node.time_in_force());
            }
        }
        let [bids, asks] = sides;
        let book = Self {
            market_id,
            bids,
            asks,
            orders,
            reservations,
            arena,
            queue_priority_allocator,
            stop_book,
            stop_priority_allocator,
            regular_expiry,
            reference_prices,
            price_band,
        };
        book.validate_snapshot_invariants()
            .map_err(|_| SnapshotWireError)?;
        Ok(book)
    }
}
