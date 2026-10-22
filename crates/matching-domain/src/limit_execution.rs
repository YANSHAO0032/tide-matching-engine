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
/// 不携带持久化封装、资金释放或完整下单生命周期；事件保持成交原序，
/// 仅在 IOC 有正余量时追加一次 OrderCanceled，绝不产生 OrderOpened。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LimitImmediateExecution {
    events: Vec<EngineEvent>,
    canceled_remaining: Option<Qty>,
}

impl LimitImmediateExecution {
    /// 包装按提交顺序生成的成交，并在尾部记录正余量取消。
    ///
    /// 调用方须保证成交已提交、吃单方身份一致且数量守恒；本构造只保存载荷，
    /// 不校验 TradeId 唯一性、成交总量或 FOK 的全部成交或零成交条件。
    pub fn new(
        order_id: OrderId,
        trades: Vec<TradeEvent>,
        canceled_remaining: Option<Qty>,
    ) -> Self {
        Self::from_events(
            order_id,
            trades.into_iter().map(EngineEvent::Trade).collect(),
            canceled_remaining,
        )
    }

    /// 保留已提交成交及挂单冰山生命周期的顺序，最后追加 IOC 正余量取消。
    ///
    /// 同 `new` 一样只包装载荷；调用方负责身份、事件语义、数量守恒和唯一性。
    pub fn from_events(
        order_id: OrderId,
        mut events: Vec<EngineEvent>,
        canceled_remaining: Option<Qty>,
    ) -> Self {
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
/// 限价单支持 GTC/IOC/FOK，冰山支持 GTC；PostOnly 允许限价单/冰山 GTC。
/// 两类市价单均仅开放 IOC，因此市价单 FOK 在此范围暂禁。止损订单、
/// DAY/GTD 与 ReduceOnly 的执行支持尚未接入，返回 InvalidOrderCombination。
/// 这只是命令适配层可调用的结构定义校验，不代替数值、簿或资金校验。
pub fn validate_basic_order_combination(
    kind: OrderType,
    tif: TimeInForce,
    flags: OrderFlags,
) -> Result<(), RejectReason> {
    if flags.reduce_only
        || (flags.post_only
            && (!matches!(kind, OrderType::Limit | OrderType::Iceberg) || tif != TimeInForce::Gtc))
    {
        return Err(RejectReason::InvalidOrderCombination);
    }
    match (kind, tif) {
        (OrderType::Limit, TimeInForce::Gtc | TimeInForce::Ioc | TimeInForce::Fok)
        | (OrderType::Iceberg, TimeInForce::Gtc)
        | (OrderType::Market, TimeInForce::Ioc) => Ok(()),
        _ => Err(RejectReason::InvalidOrderCombination),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LogicalTime, Price, Side, TradeId};

    #[test]
    fn iceberg_events_remain_before_ioc_cancel() {
        let maker = OrderId::new(1);
        let incoming = OrderId::new(2);
        let qty = Qty::try_new(3).unwrap();
        let events = vec![
            EngineEvent::OrderPartiallyFilled {
                order_id: maker,
                remaining: Qty::try_new(7).unwrap(),
            },
            EngineEvent::IcebergRefreshed {
                order_id: maker,
                visible: qty,
            },
        ];
        let result = LimitImmediateExecution::from_events(incoming, events.clone(), Some(qty));
        assert_eq!(&result.events()[..2], &events);
        assert_eq!(
            result.events()[2],
            EngineEvent::OrderCanceled {
                order_id: incoming,
                remaining: qty,
                reason: CancelReason::IocRemainder,
            }
        );
        assert_eq!(
            LimitImmediateExecution::from_events(incoming, events.clone(), None).events(),
            &events
        );
    }

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
                                OrderType::Iceberg => tif == TimeInForce::Gtc,
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
