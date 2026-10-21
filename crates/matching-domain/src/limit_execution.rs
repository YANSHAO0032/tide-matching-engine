use crate::{
    EngineEvent, OrderFlags, OrderId, OrderType, Qty, RejectReason, TimeInForce, TradeEvent,
};

/// 当前产生的取消原因；编号与规格的稳定协议表一致。
#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelReason {
    /// IOC 完成立即可执行成交后，取消尚未入簿的余量。
    IocRemainder = 1114,
}

/// 限价立即执行的成交事件及已取消余量。
///
/// 不携带 durable envelope、资金释放或完整 Place 生命周期；事件保持成交原序，
/// 仅在 IOC 有正余量时追加一次 OrderCanceled，绝不产生 OrderOpened。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LimitImmediateExecution {
    events: Vec<EngineEvent>,
    canceled_remaining: Option<Qty>,
}

impl LimitImmediateExecution {
    /// 包装按提交顺序生成的成交，并在尾部记录正余量取消。
    ///
    /// 调用方须保证成交已提交、taker 身份一致且数量守恒；本构造只保存载荷，
    /// 不校验 TradeId 唯一性、成交总量或 FOK 的 all-or-zero 条件。
    pub fn new(
        order_id: OrderId,
        trades: Vec<TradeEvent>,
        canceled_remaining: Option<Qty>,
    ) -> Self {
        let mut events: Vec<_> = trades.into_iter().map(EngineEvent::Trade).collect();
        if let Some(remaining) = canceled_remaining {
            events.push(EngineEvent::OrderCanceled {
                order_id,
                remaining,
                reason: CancelReason::IocRemainder,
            });
        }
        Self {
            events,
            canceled_remaining,
        }
    }

    /// 已提交的有序成交与末尾可选取消事件。
    pub fn events(&self) -> &[EngineEvent] {
        &self.events
    }

    /// 未成交并取消的正数量；合法订单簿执行结果中的 None 表示全部成交。
    pub fn canceled_remaining(&self) -> Option<Qty> {
        self.canceled_remaining
    }
}

/// 校验当前基础订单执行范围的类型/TIF/标志组合。
///
/// Limit 支持 GTC/IOC/FOK，PostOnly 仅允许 Limit GTC。两类 Market 均仅开放
/// IOC，因此 Market FOK（包括按 base 下单）在此范围暂禁。Stop、Iceberg、
/// DAY/GTD 与 ReduceOnly 的执行支持尚未接入，返回 InvalidOrderCombination。
/// 这只是命令适配层可调用的 schema 校验，不代替数值、簿或资金校验。
pub fn validate_basic_order_combination(
    kind: OrderType,
    tif: TimeInForce,
    flags: OrderFlags,
) -> Result<(), RejectReason> {
    if flags.reduce_only
        || (flags.post_only && (kind != OrderType::Limit || tif != TimeInForce::Gtc))
    {
        return Err(RejectReason::InvalidOrderCombination);
    }
    match (kind, tif) {
        (OrderType::Limit, TimeInForce::Gtc | TimeInForce::Ioc | TimeInForce::Fok)
        | (OrderType::Market, TimeInForce::Ioc) => Ok(()),
        _ => Err(RejectReason::InvalidOrderCombination),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LogicalTime, Price, Side, TradeId};

    #[test]
    fn limit_execution_preserves_trade_order_and_cancels_only_positive_remainder() {
        let id = OrderId::new(9);
        let trade = |n| TradeEvent {
            trade_id: TradeId::new(n),
            maker_order_id: OrderId::new(n),
            taker_order_id: id,
            price: Price::try_new(100).unwrap(),
            qty: Qty::try_new(2).unwrap(),
            maker_side: Side::Sell,
        };
        let trades = vec![trade(2), trade(1)];
        let qty = Qty::try_new(3).unwrap();
        let result = LimitImmediateExecution::new(id, trades.clone(), Some(qty));
        assert_eq!(
            result.events(),
            &[
                EngineEvent::Trade(trade(2)),
                EngineEvent::Trade(trade(1)),
                EngineEvent::OrderCanceled {
                    order_id: id,
                    remaining: qty,
                    reason: CancelReason::IocRemainder
                },
            ]
        );
        assert_eq!(result.canceled_remaining(), Some(qty));
        let full = LimitImmediateExecution::new(id, trades, None);
        assert_eq!(full.events(), &result.events()[..2]);
        assert_eq!(full.canceled_remaining(), None);
        assert_eq!(CancelReason::IocRemainder as u16, 1114);
        let empty = LimitImmediateExecution::new(id, Vec::new(), Some(qty));
        assert_eq!(empty.events(), &result.events()[2..]);
    }

    #[test]
    fn basic_combination_matrix_rejects_unsupported_flags_and_tifs() {
        for kind in [
            OrderType::Limit,
            OrderType::Market,
            OrderType::StopLimit,
            OrderType::StopMarket,
            OrderType::Iceberg,
        ] {
            for tif in [
                TimeInForce::Gtc,
                TimeInForce::Ioc,
                TimeInForce::Fok,
                TimeInForce::Day { session_id: 1 },
                TimeInForce::Gtd {
                    expire_at: LogicalTime::new(1),
                },
            ] {
                for post_only in [false, true] {
                    for reduce_only in [false, true] {
                        let allowed = !reduce_only
                            && match kind {
                                OrderType::Limit => match tif {
                                    TimeInForce::Gtc => true,
                                    TimeInForce::Ioc | TimeInForce::Fok => !post_only,
                                    _ => false,
                                },
                                OrderType::Market => tif == TimeInForce::Ioc && !post_only,
                                _ => false,
                            };
                        assert_eq!(
                            validate_basic_order_combination(
                                kind,
                                tif,
                                OrderFlags {
                                    post_only,
                                    reduce_only
                                }
                            ),
                            if allowed {
                                Ok(())
                            } else {
                                Err(RejectReason::InvalidOrderCombination)
                            },
                            "{kind:?} {tif:?} post={post_only} reduce={reduce_only}"
                        );
                    }
                }
            }
        }
    }
}
