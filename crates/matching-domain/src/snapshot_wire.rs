//! 快照载荷的有界大端基础编码及领域值编解码。

/// 编码容量、输入截断或尾部校验失败。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotWireError;

/// 在调用方给定的总字节上限内顺序构造快照载荷。
pub struct SnapshotEncoder {
    /// 尚在构造的编码字节。
    bytes: Vec<u8>,
    /// 总编码长度上限。
    limit: usize,
}
impl SnapshotEncoder {
    /// 在一次容量检查后追加完整字节，失败时不改变已有编码。
    pub fn raw_bytes(&mut self, value: &[u8]) -> Result<(), SnapshotWireError> {
        self.raw(value)
    }
    /// 创建空编码器，并固定允许的总字节数。
    pub fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
        }
    }
    /// 返回当前还允许追加的字节数。
    pub(crate) fn remaining_capacity(&self) -> usize {
        self.limit - self.bytes.len()
    }
    /// 追加一字节整数。
    pub fn u8(&mut self, value: u8) -> Result<(), SnapshotWireError> {
        self.raw(&[value])
    }
    /// 追加大端无符号 16 位整数。
    pub fn u16(&mut self, value: u16) -> Result<(), SnapshotWireError> {
        self.raw(&value.to_be_bytes())
    }
    /// 追加大端无符号 32 位整数。
    pub fn u32(&mut self, value: u32) -> Result<(), SnapshotWireError> {
        self.raw(&value.to_be_bytes())
    }
    /// 追加大端无符号 64 位整数。
    pub fn u64(&mut self, value: u64) -> Result<(), SnapshotWireError> {
        self.raw(&value.to_be_bytes())
    }
    /// 追加大端有符号 64 位整数。
    pub fn i64(&mut self, value: i64) -> Result<(), SnapshotWireError> {
        self.raw(&value.to_be_bytes())
    }
    /// 追加大端无符号 128 位整数。
    pub fn u128(&mut self, value: u128) -> Result<(), SnapshotWireError> {
        self.raw(&value.to_be_bytes())
    }
    /// 使用 32 位长度前缀追加字节；失败不改变已有编码。
    pub fn bytes(&mut self, value: &[u8]) -> Result<(), SnapshotWireError> {
        let len = u32::try_from(value.len()).map_err(|_| SnapshotWireError)?;
        let additional = 4_usize.checked_add(value.len()).ok_or(SnapshotWireError)?;
        self.reserve(additional)?;
        self.bytes.extend_from_slice(&len.to_be_bytes());
        self.bytes.extend_from_slice(value);
        Ok(())
    }
    /// 完成编码并返回已构造字节。
    pub fn finish(self) -> Vec<u8> {
        self.bytes
    }
    /// 预留空间并检查总上限；失败不改变可见字节。
    fn reserve(&mut self, extra: usize) -> Result<(), SnapshotWireError> {
        if self
            .bytes
            .len()
            .checked_add(extra)
            .is_none_or(|size| size > self.limit)
        {
            return Err(SnapshotWireError);
        }
        self.bytes.try_reserve(extra).map_err(|_| SnapshotWireError)
    }
    /// 在检查容量后原样追加固定宽度字段。
    fn raw(&mut self, value: &[u8]) -> Result<(), SnapshotWireError> {
        self.reserve(value.len())?;
        self.bytes.extend_from_slice(value);
        Ok(())
    }
}

/// 用边界检查读取快照载荷，不拥有或改变源字节。
pub struct SnapshotDecoder<'a> {
    /// 待读取的数据。
    bytes: &'a [u8],
    /// 当前已消费的字节数。
    offset: usize,
}
impl<'a> SnapshotDecoder<'a> {
    /// 检查输入长度并创建读取游标。
    pub fn new(bytes: &'a [u8], limit: usize) -> Result<Self, SnapshotWireError> {
        if bytes.len() > limit {
            return Err(SnapshotWireError);
        }
        Ok(Self { bytes, offset: 0 })
    }
    /// 读取一字节整数。
    pub fn u8(&mut self) -> Result<u8, SnapshotWireError> {
        Ok(self.take::<1>()?[0])
    }
    /// 读取大端无符号 16 位整数。
    pub fn u16(&mut self) -> Result<u16, SnapshotWireError> {
        Ok(u16::from_be_bytes(self.take()?))
    }
    /// 读取大端无符号 32 位整数。
    pub fn u32(&mut self) -> Result<u32, SnapshotWireError> {
        Ok(u32::from_be_bytes(self.take()?))
    }
    /// 读取大端无符号 64 位整数。
    pub fn u64(&mut self) -> Result<u64, SnapshotWireError> {
        Ok(u64::from_be_bytes(self.take()?))
    }
    /// 读取大端有符号 64 位整数。
    pub fn i64(&mut self) -> Result<i64, SnapshotWireError> {
        Ok(i64::from_be_bytes(self.take()?))
    }
    /// 读取大端无符号 128 位整数。
    pub fn u128(&mut self) -> Result<u128, SnapshotWireError> {
        Ok(u128::from_be_bytes(self.take()?))
    }
    /// 读取带 32 位长度前缀的字节切片。
    pub fn bytes(&mut self) -> Result<&'a [u8], SnapshotWireError> {
        let start = self.offset;
        let header = start.checked_add(4).ok_or(SnapshotWireError)?;
        let prefix: [u8; 4] = self
            .bytes
            .get(start..header)
            .ok_or(SnapshotWireError)?
            .try_into()
            .map_err(|_| SnapshotWireError)?;
        let length = usize::try_from(u32::from_be_bytes(prefix)).map_err(|_| SnapshotWireError)?;
        let end = header.checked_add(length).ok_or(SnapshotWireError)?;
        let result = self.bytes.get(header..end).ok_or(SnapshotWireError)?;
        self.offset = end;
        Ok(result)
    }
    /// 返回尚未读取的字节数。
    pub fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }
    /// 只在完整消费输入时确认成功。
    pub fn finish(self) -> Result<(), SnapshotWireError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(SnapshotWireError)
        }
    }
    /// 按固定宽度读取并推进游标，失败不推进。
    fn take<const N: usize>(&mut self) -> Result<[u8; N], SnapshotWireError> {
        let end = self.offset.checked_add(N).ok_or(SnapshotWireError)?;
        let result = self
            .bytes
            .get(self.offset..end)
            .ok_or(SnapshotWireError)?
            .try_into()
            .map_err(|_| SnapshotWireError)?;
        self.offset = end;
        Ok(result)
    }
}

/// 将合法普通限价订单以固定字段顺序写入快照载荷。
///
/// 不支持的自成交模式及容量不足均在写入前拒绝。
pub fn encode_limit_order(
    writer: &mut SnapshotEncoder,
    order: &crate::LimitOrder,
) -> Result<(), SnapshotWireError> {
    use crate::{Side, StpMode};
    let mode = match order.stp_mode {
        StpMode::None => 0,
        StpMode::CancelTaker => 1,
        StpMode::CancelMaker => 2,
        StpMode::CancelBoth => 3,
        StpMode::DecrementAndCancel => return Err(SnapshotWireError),
    };
    let side = match order.side {
        Side::Buy => 0,
        Side::Sell => 1,
    };
    writer.reserve(50)?;
    writer.u128(order.order_id.0)?;
    writer.u64(order.user_id.0)?;
    writer.u64(order.stp_key.get())?;
    writer.u8(mode)?;
    writer.u8(side)?;
    writer.i64(order.price.get())?;
    writer.u64(order.qty.get())
}

/// 解码普通限价订单并验证标记、价格、数量及自成交模式。
pub fn decode_limit_order(
    reader: &mut SnapshotDecoder<'_>,
) -> Result<crate::LimitOrder, SnapshotWireError> {
    use crate::{LimitOrder, OrderId, Price, Qty, Side, StpKey, StpMode, UserId};
    let order_id = OrderId(reader.u128()?);
    let user_id = UserId(reader.u64()?);
    let stp_key = StpKey::new(reader.u64()?);
    let stp_mode = match reader.u8()? {
        0 => StpMode::None,
        1 => StpMode::CancelTaker,
        2 => StpMode::CancelMaker,
        3 => StpMode::CancelBoth,
        _ => return Err(SnapshotWireError),
    };
    let side = match reader.u8()? {
        0 => Side::Buy,
        1 => Side::Sell,
        _ => return Err(SnapshotWireError),
    };
    let price = Price::try_new(reader.i64()?).map_err(|_| SnapshotWireError)?;
    let qty = Qty::try_new(reader.u64()?).map_err(|_| SnapshotWireError)?;
    Ok(LimitOrder {
        order_id,
        user_id,
        stp_key,
        stp_mode,
        side,
        price,
        qty,
    })
}

/// 将止损订单完整编码；非法组合或容量不足时不改变外层载荷。
pub fn encode_stop_order(
    writer: &mut SnapshotEncoder,
    order: &crate::StopOrder,
) -> Result<(), SnapshotWireError> {
    use crate::{PriceReferenceKind, StopMarketQuantity, StopOrder, TimeInForce, TriggerExpiry};
    if order.validate_stp_mode().is_err()
        || order.trigger_reference() == PriceReferenceKind::StaticOpening
    {
        return Err(SnapshotWireError);
    }
    let mut w = SnapshotEncoder::new(256);
    let (id, user, key, mode, reservation, fee, side, stop, tif, expiry) = match order {
        StopOrder::Limit(o) => (
            o.order_id,
            o.user_id,
            o.stp_key,
            o.stp_mode,
            o.reservation_id,
            o.fee_schedule_version,
            o.side,
            o.stop_price,
            o.child_tif,
            o.trigger_expiry,
        ),
        StopOrder::Market(o) => (
            o.order_id,
            o.user_id,
            o.stp_key,
            o.stp_mode,
            o.reservation_id,
            o.fee_schedule_version,
            o.side,
            o.stop_price,
            o.child_tif,
            o.trigger_expiry,
        ),
    };
    w.u8(match order {
        StopOrder::Limit(_) => 0,
        StopOrder::Market(_) => 1,
    })?;
    w.u8(order.trigger_reference() as u8)?;
    w.u128(id.get())?;
    w.u64(user.get())?;
    w.u64(key.get())?;
    w.u8(match mode {
        crate::StpMode::None => 0,
        crate::StpMode::CancelTaker => 1,
        crate::StpMode::CancelMaker => 2,
        crate::StpMode::CancelBoth => 3,
        _ => return Err(SnapshotWireError),
    })?;
    w.u128(reservation.get())?;
    w.u64(fee)?;
    w.u8(match side {
        crate::Side::Buy => 0,
        crate::Side::Sell => 1,
    })?;
    w.i64(stop.get())?;
    match order {
        StopOrder::Limit(o) => {
            w.i64(o.price.get())?;
            w.u64(o.qty.get())?;
        }
        StopOrder::Market(o) => {
            match &o.market_protection {
                None => w.u8(0)?,
                Some(p) => {
                    w.u8(1)?;
                    match p.max_slippage_bps {
                        None => w.u8(0)?,
                        Some(b) if b < 10_000 => {
                            w.u8(1)?;
                            w.u32(b)?
                        }
                        _ => return Err(SnapshotWireError),
                    };
                    match p.worst_price {
                        None => w.u8(0)?,
                        Some(p) => {
                            w.u8(1)?;
                            w.i64(p.get())?
                        }
                    }
                }
            }
            match o.quantity {
                StopMarketQuantity::Base(q) => {
                    w.u8(0)?;
                    w.u64(q.get())?
                }
                StopMarketQuantity::Quote(q) => {
                    w.u8(1)?;
                    w.u128(q.get())?
                }
            }
        }
    }
    match (order, tif) {
        (StopOrder::Limit(_), TimeInForce::Gtc) => w.u8(0)?,
        (StopOrder::Market(_), TimeInForce::Ioc) => w.u8(1)?,
        (StopOrder::Limit(_), TimeInForce::Day { session_id }) => {
            w.u8(2)?;
            w.u64(session_id)?
        }
        (StopOrder::Limit(_), TimeInForce::Gtd { expire_at }) => {
            w.u8(3)?;
            w.u64(expire_at.get())?
        }
        _ => return Err(SnapshotWireError),
    }
    match expiry {
        TriggerExpiry::Gtc => w.u8(0)?,
        TriggerExpiry::Day { session_id } => {
            w.u8(1)?;
            w.u64(session_id)?
        }
        TriggerExpiry::Gtd { expire_at } => {
            w.u8(2)?;
            w.u64(expire_at.get())?
        }
    }
    writer.bytes(&w.finish())
}

/// 解码止损订单并拒绝未知标签、无效数值及不支持的组合。
pub fn decode_stop_order(
    reader: &mut SnapshotDecoder<'_>,
) -> Result<crate::StopOrder, SnapshotWireError> {
    use crate::{
        LogicalTime, MarketProtection, OrderId, Price, PriceReferenceKind, Qty, QuoteBudget,
        ReservationId, Side, StopLimitOrder, StopMarketOrder, StopMarketQuantity, StopOrder,
        StpKey, StpMode, TimeInForce, TriggerExpiry, UserId,
    };
    let bytes = reader.bytes()?;
    let mut r = SnapshotDecoder::new(bytes, 256)?;
    let kind = r.u8()?;
    let reference = match r.u8()? {
        1 => PriceReferenceKind::LastTrade,
        2 => PriceReferenceKind::MarkPrice,
        3 => PriceReferenceKind::IndexPrice,
        _ => return Err(SnapshotWireError),
    };
    let id = OrderId::new(r.u128()?);
    let user = UserId::new(r.u64()?);
    let key = StpKey::new(r.u64()?);
    let mode = match r.u8()? {
        0 => StpMode::None,
        1 => StpMode::CancelTaker,
        2 => StpMode::CancelMaker,
        3 => StpMode::CancelBoth,
        _ => return Err(SnapshotWireError),
    };
    let reservation = ReservationId::new(r.u128()?);
    let fee = r.u64()?;
    let side = match r.u8()? {
        0 => Side::Buy,
        1 => Side::Sell,
        _ => return Err(SnapshotWireError),
    };
    let stop = Price::try_new(r.i64()?).map_err(|_| SnapshotWireError)?;
    let mut limit = None;
    let mut market = None;
    match kind {
        0 => {
            let p = Price::try_new(r.i64()?).map_err(|_| SnapshotWireError)?;
            let q = Qty::try_new(r.u64()?).map_err(|_| SnapshotWireError)?;
            limit = Some((p, q));
        }
        1 => {
            let protection = match r.u8()? {
                0 => None,
                1 => {
                    let b = match r.u8()? {
                        0 => None,
                        1 => {
                            let b = r.u32()?;
                            if b >= 10_000 {
                                return Err(SnapshotWireError);
                            }
                            Some(b)
                        }
                        _ => return Err(SnapshotWireError),
                    };
                    let p = match r.u8()? {
                        0 => None,
                        1 => Some(Price::try_new(r.i64()?).map_err(|_| SnapshotWireError)?),
                        _ => return Err(SnapshotWireError),
                    };
                    Some(MarketProtection {
                        max_slippage_bps: b,
                        worst_price: p,
                    })
                }
                _ => return Err(SnapshotWireError),
            };
            let q = match r.u8()? {
                0 => {
                    StopMarketQuantity::Base(Qty::try_new(r.u64()?).map_err(|_| SnapshotWireError)?)
                }
                1 => StopMarketQuantity::Quote(
                    QuoteBudget::try_new(r.u128()?).map_err(|_| SnapshotWireError)?,
                ),
                _ => return Err(SnapshotWireError),
            };
            market = Some((protection, q));
        }
        _ => return Err(SnapshotWireError),
    }
    let tif = match r.u8()? {
        0 => TimeInForce::Gtc,
        1 => TimeInForce::Ioc,
        2 => TimeInForce::Day {
            session_id: r.u64()?,
        },
        3 => TimeInForce::Gtd {
            expire_at: LogicalTime::new(r.u64()?),
        },
        _ => return Err(SnapshotWireError),
    };
    let expiry = match r.u8()? {
        0 => TriggerExpiry::Gtc,
        1 => TriggerExpiry::Day {
            session_id: r.u64()?,
        },
        2 => TriggerExpiry::Gtd {
            expire_at: LogicalTime::new(r.u64()?),
        },
        _ => return Err(SnapshotWireError),
    };
    r.finish()?;
    match (limit, market, tif) {
        (
            Some((price, qty)),
            None,
            TimeInForce::Gtc | TimeInForce::Day { .. } | TimeInForce::Gtd { .. },
        ) => Ok(StopOrder::Limit(StopLimitOrder {
            trigger_reference: reference,
            order_id: id,
            user_id: user,
            stp_key: key,
            stp_mode: mode,
            reservation_id: reservation,
            fee_schedule_version: fee,
            side,
            stop_price: stop,
            price,
            qty,
            child_tif: tif,
            trigger_expiry: expiry,
        })),
        (None, Some((market_protection, quantity)), TimeInForce::Ioc) => {
            Ok(StopOrder::Market(StopMarketOrder {
                trigger_reference: reference,
                market_protection,
                order_id: id,
                user_id: user,
                stp_key: key,
                stp_mode: mode,
                reservation_id: reservation,
                fee_schedule_version: fee,
                side,
                stop_price: stop,
                quantity,
                child_tif: tif,
                trigger_expiry: expiry,
            }))
        }
        _ => Err(SnapshotWireError),
    }
}

/// 按参考来源固定顺序编码各来源最新价格与序号。
pub fn encode_reference_state(
    writer: &mut SnapshotEncoder,
    state: &crate::PriceReferenceState,
) -> Result<(), SnapshotWireError> {
    use crate::PriceReferenceKind;
    let kinds = [
        PriceReferenceKind::LastTrade,
        PriceReferenceKind::MarkPrice,
        PriceReferenceKind::IndexPrice,
        PriceReferenceKind::StaticOpening,
    ];
    let count = kinds
        .iter()
        .filter(|kind| state.latest(**kind).is_some())
        .count();
    writer.reserve(1 + count * 17)?;
    writer.u8(u8::try_from(count).map_err(|_| SnapshotWireError)?)?;
    for kind in kinds {
        if let Some(value) = state.latest(kind) {
            writer.u8(kind as u8)?;
            writer.u64(value.source_seq)?;
            writer.i64(value.price.get())?;
        }
    }
    Ok(())
}

/// 按严格递增的来源标记恢复参考价格，并拒绝重复与未知来源。
pub fn decode_reference_state(
    reader: &mut SnapshotDecoder<'_>,
) -> Result<crate::PriceReferenceState, SnapshotWireError> {
    use crate::{Price, PriceReference, PriceReferenceKind, PriceReferenceState};
    let count = reader.u8()?;
    if count > 4 {
        return Err(SnapshotWireError);
    }
    let mut previous = 0;
    let mut state = PriceReferenceState::default();
    for _ in 0..count {
        let tag = reader.u8()?;
        if tag <= previous {
            return Err(SnapshotWireError);
        }
        previous = tag;
        let kind = match tag {
            1 => PriceReferenceKind::LastTrade,
            2 => PriceReferenceKind::MarkPrice,
            3 => PriceReferenceKind::IndexPrice,
            4 => PriceReferenceKind::StaticOpening,
            _ => return Err(SnapshotWireError),
        };
        let source_seq = reader.u64()?;
        let price = Price::try_new(reader.i64()?).map_err(|_| SnapshotWireError)?;
        state
            .advance(PriceReference {
                kind,
                source_seq,
                price,
            })
            .map_err(|_| SnapshotWireError)?;
    }
    Ok(state)
}

/// 写入已验证的市场数值配置，不重复保存可计算的派生值。
pub fn encode_market_math(
    writer: &mut SnapshotEncoder,
    math: &crate::MarketMath,
) -> Result<(), SnapshotWireError> {
    writer.reserve(19)?;
    writer.u8(math.price_scale())?;
    writer.u8(math.qty_scale())?;
    writer.u8(math.quote_scale())?;
    writer.i64(math.tick_size())?;
    writer.u64(math.lot_size())
}

/// 重建市场数值配置并重新执行精度与最小本金约束。
pub fn decode_market_math(
    reader: &mut SnapshotDecoder<'_>,
) -> Result<crate::MarketMath, SnapshotWireError> {
    let price_scale = reader.u8()?;
    let qty_scale = reader.u8()?;
    let quote_scale = reader.u8()?;
    let tick = reader.i64()?;
    let lot = reader.u64()?;
    crate::MarketMath::try_new(price_scale, qty_scale, quote_scale, tick, lot)
        .map_err(|_| SnapshotWireError)
}

/// 写入已冻结的拒绝原因编号。
pub fn encode_reject_reason(
    writer: &mut SnapshotEncoder,
    reason: crate::RejectReason,
) -> Result<(), SnapshotWireError> {
    writer.u16(reason as u16)
}

/// 读取拒绝原因编号，未知编号必须拒绝恢复。
pub fn decode_reject_reason(
    reader: &mut SnapshotDecoder<'_>,
) -> Result<crate::RejectReason, SnapshotWireError> {
    use crate::RejectReason::*;
    Ok(match reader.u16()? {
        1001 => MarketNotFound,
        1002 => MarketNotOpen,
        1003 => MarketHalted,
        1004 => MarketSuspended,
        1005 => MarketClosed,
        1006 => PreOpenWouldCross,
        1007 => KillSwitchActive,
        1008 => MigrationInProgress,
        1009 => StaleRouteEpoch,
        1010 => InvalidMarketStateTransition,
        1011 => MarketConfigInvalidTick,
        1012 => MarketConfigInvalidLot,
        1013 => MarketConfigUnsupportedScale,
        1014 => MarketConfigNonIntegralQuoteUnit,
        1015 => MarketConfigArithmeticOverflow,
        2001 => InvalidOrderCombination,
        2002 => InvalidPrice,
        2003 => InvalidQty,
        2004 => InvalidTick,
        2005 => InvalidLot,
        2006 => MinNotional,
        2007 => MaxNotional,
        2008 => DuplicateOrderId,
        2009 => DuplicateClientOrderId,
        2010 => OrderNotFound,
        2011 => InvalidIceberg,
        2013 => InvalidExpireTime,
        2014 => InvalidSession,
        2015 => PostOnlyWouldCross,
        2016 => FokInsufficientLiquidity,
        3001 => PriceBandExceeded,
        3002 => ReferencePriceUnavailable,
        3003 => InvalidReferencePriceSource,
        3004 => StaleReferencePriceUpdate,
        3005 => MarketProtectionRequired,
        3006 => MarketProtectionExceeded,
        3007 => InvalidSlippageBps,
        4001 => InsufficientReservation,
        4002 => InvalidReservation,
        4003 => FeeScheduleNotFound,
        4004 => FeeScheduleMismatch,
        4005 => StaleUserControlEpoch,
        5001 => ArithmeticOverflow,
        5002 => EngineBusy,
        5005 => StalePrimaryEpoch,
        5006 => PrioritySpaceExhaustion,
        _ => return Err(SnapshotWireError),
    })
}
