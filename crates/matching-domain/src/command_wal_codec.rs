//! 正式命令的版本化、有限长度大端编码。
use crate::*;

/// 命令载荷编码错误，包括输入过长、截断、未知标记和无效值。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommandCodecError;
/// 与 WAL 单条载荷上限一致的最大命令长度。
const MAX: usize = 16 * 1024 * 1024;

/// 将完整正式命令编码为可重放字节；不验证业务授权。
pub fn encode_wal_command(c: &CommandEnvelope) -> Result<Vec<u8>, CommandCodecError> {
    let mut w = Writer(vec![1]);
    w.u128(c.command_id.0);
    w.u64(c.session_id);
    w.u64(c.client_seq);
    w.opt(c.market_id, |w, v| w.u32(v.0));
    w.opt(c.user_id, |w, v| w.u64(v.0));
    w.opt(c.route_epoch, |w, v| w.u64(v));
    w.opt(c.client_timestamp, |w, v| w.u64(v));
    w.opt(c.received_at_unix_ns, |w, v| w.u64(v));
    match &c.payload {
        Command::ApplyUserControlFenceShard(f) => {
            w.u8(0);
            w.u128(f.parent_command_id.0);
            w.u64(f.user_id.0);
            w.u64(f.new_epoch);
            w.u8(match f.action {
                UserFenceAction::FenceOnly => 0,
                UserFenceAction::FenceAndCancelAll => 1,
            });
            w.u64(f.expected_primary_epoch);
        }
        Command::MassCancelShard(m) => {
            w.u8(1);
            match m.scope {
                ShardCancelScope::User { user_id } => {
                    w.u8(0);
                    w.u64(user_id.0);
                }
                ShardCancelScope::Market { market_id } => {
                    w.u8(1);
                    w.u32(market_id.0);
                }
                ShardCancelScope::UserMarket { user_id, market_id } => {
                    w.u8(2);
                    w.u64(user_id.0);
                    w.u32(market_id.0);
                }
                ShardCancelScope::Session {
                    user_id,
                    session_id,
                } => {
                    w.u8(3);
                    w.u64(user_id.0);
                    w.u64(session_id.0);
                }
                ShardCancelScope::All => w.u8(4),
            }
        }
        Command::PlaceLimit(p) => {
            w.u8(2);
            let o = &p.order;
            w.u128(o.order_id.0);
            w.u64(o.user_id.0);
            w.u64(o.stp_key.get());
            w.u8(match o.stp_mode {
                StpMode::None => 0,
                StpMode::CancelTaker => 1,
                StpMode::CancelMaker => 2,
                StpMode::CancelBoth => 3,
                StpMode::DecrementAndCancel => 4,
            });
            w.u8(match o.side {
                Side::Buy => 0,
                Side::Sell => 1,
            });
            w.i64(o.price.get());
            w.u64(o.qty.get());
            match &p.client_order_id {
                None => w.u8(0),
                Some(s) => {
                    w.u8(1);
                    w.bytes(s.as_bytes())?;
                }
            }
            w.u64(p.user_control_epoch);
            w.u8(u8::from(p.post_only));
            let t = &p.reservation;
            w.u128(t.token_id);
            w.u128(t.command_id.0);
            w.u128(t.reservation_id.0);
            w.u128(t.order_id.0);
            w.u64(t.user_id.0);
            w.u32(t.market_id.0);
            let reserves_size = t.reserves.len().checked_mul(20).ok_or(CommandCodecError)?;
            if w.0
                .len()
                .checked_add(4)
                .and_then(|n| n.checked_add(reserves_size))
                .and_then(|n| n.checked_add(80))
                .is_none_or(|n| n > MAX)
            {
                return Err(CommandCodecError);
            }
            w.u32(u32::try_from(t.reserves.len()).map_err(|_| CommandCodecError)?);
            for r in &t.reserves {
                w.u32(r.asset.0);
                w.u128(r.atoms);
            }
            w.u64(t.fee_schedule_version);
            w.u32(t.fee_tier_id);
            w.u32(t.risk_key_id);
            w.0.extend_from_slice(&t.signature);
        }
    }
    if w.0.len() > MAX {
        return Err(CommandCodecError);
    }
    Ok(w.0)
}
/// 从完整字节解码正式命令，拒绝尾部多余字节和未知版本。
pub fn decode_wal_command(bytes: &[u8]) -> Result<CommandEnvelope, CommandCodecError> {
    if bytes.len() > MAX {
        return Err(CommandCodecError);
    }
    let mut r = Reader { bytes, pos: 0 };
    if r.u8()? != 1 {
        return Err(CommandCodecError);
    }
    let command_id = CommandId(r.u128()?);
    let session_id = r.u64()?;
    let client_seq = r.u64()?;
    let market_id = r.opt(|r| Ok(MarketId(r.u32()?)))?;
    let user_id = r.opt(|r| Ok(UserId(r.u64()?)))?;
    let route_epoch = r.opt(|r| r.u64())?;
    let client_timestamp = r.opt(|r| r.u64())?;
    let received_at_unix_ns = r.opt(|r| r.u64())?;
    let payload = match r.u8()? {
        0 => Command::ApplyUserControlFenceShard(ApplyUserControlFenceShard {
            parent_command_id: CommandId(r.u128()?),
            user_id: UserId(r.u64()?),
            new_epoch: r.u64()?,
            action: match r.u8()? {
                0 => UserFenceAction::FenceOnly,
                1 => UserFenceAction::FenceAndCancelAll,
                _ => return Err(CommandCodecError),
            },
            expected_primary_epoch: r.u64()?,
        }),
        1 => Command::MassCancelShard(MassCancelShard {
            scope: match r.u8()? {
                0 => ShardCancelScope::User {
                    user_id: UserId(r.u64()?),
                },
                1 => ShardCancelScope::Market {
                    market_id: MarketId(r.u32()?),
                },
                2 => ShardCancelScope::UserMarket {
                    user_id: UserId(r.u64()?),
                    market_id: MarketId(r.u32()?),
                },
                3 => ShardCancelScope::Session {
                    user_id: UserId(r.u64()?),
                    session_id: OriginSessionId(r.u64()?),
                },
                4 => ShardCancelScope::All,
                _ => return Err(CommandCodecError),
            },
        }),
        2 => {
            let order = LimitOrder {
                order_id: OrderId(r.u128()?),
                user_id: UserId(r.u64()?),
                stp_key: StpKey::new(r.u64()?),
                stp_mode: match r.u8()? {
                    0 => StpMode::None,
                    1 => StpMode::CancelTaker,
                    2 => StpMode::CancelMaker,
                    3 => StpMode::CancelBoth,
                    4 => StpMode::DecrementAndCancel,
                    _ => return Err(CommandCodecError),
                },
                side: match r.u8()? {
                    0 => Side::Buy,
                    1 => Side::Sell,
                    _ => return Err(CommandCodecError),
                },
                price: Price::try_new(r.i64()?).map_err(|_| CommandCodecError)?,
                qty: Qty::try_new(r.u64()?).map_err(|_| CommandCodecError)?,
            };
            let client_order_id =
                r.opt(|r| String::from_utf8(r.bytes()?.to_vec()).map_err(|_| CommandCodecError))?;
            let user_control_epoch = r.u64()?;
            let post_only = match r.u8()? {
                0 => false,
                1 => true,
                _ => return Err(CommandCodecError),
            };
            let token_id = r.u128()?;
            let token_command = CommandId(r.u128()?);
            let reservation_id = ReservationId(r.u128()?);
            let order_id = OrderId(r.u128()?);
            let token_user = UserId(r.u64()?);
            let token_market = MarketId(r.u32()?);
            let count = usize::try_from(r.u32()?).map_err(|_| CommandCodecError)?;
            if count > r.bytes.len().saturating_sub(r.pos) / 20 {
                return Err(CommandCodecError);
            }
            let mut reserves = smallvec::SmallVec::new();
            for _ in 0..count {
                reserves.push(AssetReserve {
                    asset: AssetId(r.u32()?),
                    atoms: r.u128()?,
                });
            }
            let fee_schedule_version = r.u64()?;
            let fee_tier_id = r.u32()?;
            let risk_key_id = r.u32()?;
            let signature = r.take::<64>()?;
            let reservation = ReservationToken {
                token_id,
                command_id: token_command,
                reservation_id,
                order_id,
                user_id: token_user,
                market_id: token_market,
                reserves,
                fee_schedule_version,
                fee_tier_id,
                risk_key_id,
                signature,
            };
            Command::PlaceLimit(Box::new(PlaceLimitOrder {
                order,
                client_order_id,
                user_control_epoch,
                post_only,
                reservation,
            }))
        }
        _ => return Err(CommandCodecError),
    };
    if r.pos != bytes.len() {
        return Err(CommandCodecError);
    }
    Ok(CommandEnvelope {
        command_id,
        session_id,
        client_seq,
        market_id,
        user_id,
        route_epoch,
        client_timestamp,
        received_at_unix_ns,
        payload,
    })
}
/// 编码写入器。
struct Writer(Vec<u8>);
impl Writer {
    fn u8(&mut self, v: u8) {
        self.0.push(v)
    }
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_be_bytes())
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_be_bytes())
    }
    fn i64(&mut self, v: i64) {
        self.0.extend_from_slice(&v.to_be_bytes())
    }
    fn u128(&mut self, v: u128) {
        self.0.extend_from_slice(&v.to_be_bytes())
    }
    fn opt<T>(&mut self, v: Option<T>, f: impl FnOnce(&mut Self, T)) {
        match v {
            None => self.u8(0),
            Some(v) => {
                self.u8(1);
                f(self, v)
            }
        }
    }
    fn bytes(&mut self, v: &[u8]) -> Result<(), CommandCodecError> {
        if self
            .0
            .len()
            .checked_add(4)
            .and_then(|n| n.checked_add(v.len()))
            .is_none_or(|n| n > MAX)
        {
            return Err(CommandCodecError);
        }
        self.u32(u32::try_from(v.len()).map_err(|_| CommandCodecError)?);
        self.0.extend_from_slice(v);
        Ok(())
    }
}
/// 带边界检查的解码游标。
struct Reader<'a> {
    /// 待解码的完整字节。
    bytes: &'a [u8],
    /// 下一个待读取的偏移。
    pos: usize,
}
impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N], CommandCodecError> {
        let end = self.pos.checked_add(N).ok_or(CommandCodecError)?;
        let v = self.bytes.get(self.pos..end).ok_or(CommandCodecError)?;
        self.pos = end;
        v.try_into().map_err(|_| CommandCodecError)
    }
    fn u8(&mut self) -> Result<u8, CommandCodecError> {
        Ok(self.take::<1>()?[0])
    }
    fn u32(&mut self) -> Result<u32, CommandCodecError> {
        Ok(u32::from_be_bytes(self.take()?))
    }
    fn u64(&mut self) -> Result<u64, CommandCodecError> {
        Ok(u64::from_be_bytes(self.take()?))
    }
    fn i64(&mut self) -> Result<i64, CommandCodecError> {
        Ok(i64::from_be_bytes(self.take()?))
    }
    fn u128(&mut self) -> Result<u128, CommandCodecError> {
        Ok(u128::from_be_bytes(self.take()?))
    }
    fn opt<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<T, CommandCodecError>,
    ) -> Result<Option<T>, CommandCodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(f(self)?)),
            _ => Err(CommandCodecError),
        }
    }
    fn bytes(&mut self) -> Result<&[u8], CommandCodecError> {
        let n = usize::try_from(self.u32()?).map_err(|_| CommandCodecError)?;
        let end = self.pos.checked_add(n).ok_or(CommandCodecError)?;
        let v = self.bytes.get(self.pos..end).ok_or(CommandCodecError)?;
        self.pos = end;
        Ok(v)
    }
}
