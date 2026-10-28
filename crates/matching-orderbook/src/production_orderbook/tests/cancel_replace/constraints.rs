//! 撤单替换的双模型排除、限价边界与原序事件回归。
//!
//! 当前替换载荷只有限价与冰山；上下文没有市价保护输入。
//! 本模块验证真实限价边界，不将其冒称为尚不可达的市价保护。

use super::*;
use crate::ReferenceOrderBook;
use crate::reference_orderbook::ReferencePlaceContext;

struct Pair {
    /// 独立参考订单簿实例。
    reference: ReferenceOrderBook,
    /// 待与参考模型比较的生产订单簿实例。
    production: ProductionOrderBook,
    /// 参考模型对应的金融预留登记表。
    reference_registry: ReservationRegistry,
    /// 生产模型对应的金融预留登记表。
    production_registry: ReservationRegistry,
}

impl Pair {
    fn new() -> Self {
        Self {
            reference: ReferenceOrderBook::new(),
            production: ProductionOrderBook::new(policy()),
            reference_registry: ReservationRegistry::new([]).unwrap(),
            production_registry: ReservationRegistry::new([]).unwrap(),
        }
    }

    fn rest(&mut self, input: LimitGtcOrder, iceberg: bool, math: &MarketMath) {
        let state = reserve(input.order_id.get());
        if iceberg {
            let order = IcebergOrder::try_new(input, Qty::try_new(2).unwrap()).unwrap();
            self.reference
                .place_iceberg_post_only_with_reservation(order.clone(), math, state.clone())
                .unwrap();
            self.production
                .place_iceberg_post_only_with_reservation(order, math, state)
                .unwrap();
        } else {
            self.reference
                .rest_with_reservation(input.clone(), state.clone())
                .unwrap();
            self.production.rest_with_reservation(input, state).unwrap();
        }
        self.reference_registry = ReservationRegistry::new(
            self.reference
                .test_reservations()
                .into_iter()
                .map(|(_, state)| state),
        )
        .unwrap();
        self.production_registry =
            ReservationRegistry::new(self.production.reservations.values().cloned()).unwrap();
        self.assert_equal();
    }

    fn assert_equal(&self) {
        for side in [Side::Buy, Side::Sell] {
            let reference = self.reference.orders(side);
            let mut production = ordered_nodes(&self.production, side);
            production.sort_by(|a, b| match side {
                Side::Buy => b.original_order().price.cmp(&a.original_order().price),
                Side::Sell => a.original_order().price.cmp(&b.original_order().price),
            });
            assert_eq!(reference.len(), production.len());
            for (a, b) in reference.iter().zip(production) {
                assert_eq!(a.original_order(), b.original_order());
                assert_eq!(a.visible_remaining(), b.visible_remaining());
                assert_eq!(a.hidden_remaining(), b.hidden_remaining());
                assert_eq!(a.total_remaining(), b.total_remaining());
                assert_eq!(a.peak_size(), b.peak_size());
                assert_eq!(a.time_in_force(), b.time_in_force());
                assert_eq!(a.priority(), b.priority());
            }
            assert_eq!(
                self.reference.best(side).map(|o| o.original_order().price),
                self.production.best_price(side)
            );
        }
        assert_eq!(self.reference_registry, self.production_registry);
        let mut production_shadows = self
            .production
            .reservations
            .iter()
            .map(|(id, state)| (*id, state.clone()))
            .collect::<Vec<_>>();
        production_shadows.sort_unstable_by_key(|(id, _)| *id);
        assert_eq!(self.reference.test_reservations(), production_shadows);
        self.reference_registry
            .validate_order_bindings(
                self.reference
                    .test_reservations()
                    .iter()
                    .map(|(id, state)| (*id, state)),
            )
            .unwrap();
        parity(&self.production, &self.production_registry);
        assert_eq!(validate_book(&self.production), Ok(()));
        assert_eq!(
            self.reference.test_next_queue_priority(),
            self.production.next_queue_priority()
        );
        assert_eq!(
            self.reference.next_stop_priority(),
            self.production.next_stop_priority()
        );
        assert_eq!(
            self.reference.test_reference_state(),
            &self.production.reference_prices
        );
    }

    fn execute(
        &mut self,
        r: &CancelReplaceOrder,
        math: &MarketMath,
        ids: &[TradeId],
    ) -> Result<Vec<EngineEvent>, matching_domain::CancelReplaceRejection> {
        let reference_before = self.reference.clone();
        let production_before = production_snapshot(&self.production);
        let reference_registry_before = self.reference_registry.clone();
        let production_registry_before = self.production_registry.clone();
        let reference = self.reference.plan_cancel_replace(
            r,
            &expected(),
            &self.reference_registry,
            &keys(),
            ReferencePlaceContext {
                math,
                now: LogicalTime::new(10),
                current_session_id: 7,
                trade_ids: ids,
            },
        );
        let production = self.production.plan_cancel_replace(
            r,
            &expected(),
            &self.production_registry,
            &keys(),
            context(math, ids),
        );
        // 两个计划都只能读真实前态，失败时也不能提前释放或重绑资金。
        assert_eq!(self.reference, reference_before);
        assert_eq!(production_snapshot(&self.production), production_before);
        assert_eq!(self.reference_registry, reference_registry_before);
        assert_eq!(self.production_registry, production_registry_before);
        let events = match (reference, production) {
            (Ok(a), Ok(b)) => {
                let a = self
                    .reference
                    .apply_cancel_replace(&mut self.reference_registry, a)
                    .unwrap();
                let b = self
                    .production
                    .apply_cancel_replace(&mut self.production_registry, b)
                    .unwrap();
                assert_eq!(a, b);
                assert!(
                    !a.iter()
                        .any(|e| matches!(e, EngineEvent::ReservationReleased { .. }))
                );
                Ok(a)
            }
            (Err(a), Err(b)) => {
                assert_eq!(a, b);
                assert_eq!(
                    a.unadopted_reservation,
                    r.rejection(a.reason).unadopted_reservation
                );
                Err(a)
            }
            mismatch => panic!("CancelReplace model mismatch: {mismatch:?}"),
        };
        self.assert_equal();
        events
    }
}

fn at(id: u128, side: Side, price: i64, qty: u64) -> LimitGtcOrder {
    let mut order = input(id, side, qty);
    order.price = Price::try_new(price).unwrap();
    order
}

fn opposite(side: Side) -> Side {
    if side == Side::Buy {
        Side::Sell
    } else {
        Side::Buy
    }
}

fn canceled(id: u128, qty: u64, reason: CancelReason) -> EngineEvent {
    EngineEvent::OrderCanceled {
        order_id: OrderId::new(id),
        remaining: Qty::try_new(qty).unwrap(),
        reason,
    }
}

#[test]
fn cancel_replace_cross_model_fok_excludes_regular_and_all_iceberg_old_liquidity() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for side in [Side::Buy, Side::Sell] {
        for iceberg in [false, true] {
            for new in [false, true] {
                let mut pair = Pair::new();
                pair.rest(
                    input(1, opposite(side), if iceberg { 10 } else { 5 }),
                    iceberg,
                    &math,
                );
                pair.rest(input(2, opposite(side), 5), false, &math);
                let r = request(new, side, 10, TimeInForce::Fok);
                assert_eq!(
                    pair.execute(&r, &math, &[TradeId::new(1); 10])
                        .unwrap_err()
                        .reason,
                    RejectReason::FokInsufficientLiquidity
                );
                assert_eq!(pair.production.next_queue_priority(), 2);
                assert_eq!(
                    pair.production_registry.get(ReservationId::new(101)),
                    Some(&reserve(1))
                );
                assert!(
                    !pair
                        .production_registry
                        .is_pending_r2(ReservationId::new(101))
                );
                assert!(
                    pair.production_registry
                        .get(ReservationId::new(109))
                        .is_none()
                );
            }
        }
    }
}

#[test]
fn cancel_replace_cross_model_post_only_uses_next_best_and_inclusive_crossing() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for side in [Side::Buy, Side::Sell] {
        for old_iceberg in [false, true] {
            for new_iceberg in [false, true] {
                for new in [false, true] {
                    for crossing in [false, true] {
                        let mut pair = Pair::new();
                        let old_price = if side == Side::Buy { 99 } else { 101 };
                        let next_price = if crossing {
                            100
                        } else if side == Side::Buy {
                            101
                        } else {
                            99
                        };
                        pair.rest(at(1, opposite(side), old_price, 10), old_iceberg, &math);
                        pair.rest(at(2, opposite(side), next_price, 5), false, &math);
                        assert_eq!(
                            pair.reference
                                .book_without(OrderId::new(1))
                                .unwrap()
                                .best(opposite(side))
                                .unwrap()
                                .original_order()
                                .price
                                .get(),
                            next_price
                        );
                        assert_eq!(
                            pair.production
                                .book_without(OrderId::new(1))
                                .unwrap()
                                .best_price(opposite(side))
                                .unwrap()
                                .get(),
                            next_price
                        );
                        let mut r = request(new, side, 5, TimeInForce::Gtc);
                        let flags = OrderFlags {
                            post_only: true,
                            reduce_only: false,
                        };
                        r.replacement.kind = if new_iceberg {
                            ReplacementOrderKind::Iceberg {
                                peak_size: Qty::try_new(2).unwrap(),
                                tif: TimeInForce::Gtc,
                                flags,
                            }
                        } else {
                            ReplacementOrderKind::Limit {
                                tif: TimeInForce::Gtc,
                                flags,
                            }
                        };
                        let result = pair.execute(&r, &math, &[]);
                        if crossing {
                            assert_eq!(
                                result.unwrap_err().reason,
                                RejectReason::PostOnlyWouldCross
                            );
                            assert_eq!(pair.production.next_queue_priority(), 2);
                            assert_eq!(
                                pair.production_registry.get(ReservationId::new(101)),
                                Some(&reserve(1))
                            );
                            assert!(
                                !pair
                                    .production_registry
                                    .is_pending_r2(ReservationId::new(101))
                            );
                            assert!(
                                pair.production_registry
                                    .get(ReservationId::new(109))
                                    .is_none()
                            );
                        } else {
                            assert_eq!(
                                result.unwrap(),
                                vec![canceled(1, 10, CancelReason::CancelReplace)]
                            );
                            assert_eq!(
                                pair.production.order(OrderId::new(9)).unwrap().priority(),
                                QueuePriority::new(2)
                            );
                            assert_eq!(pair.production.next_queue_priority(), 3);
                            assert_eq!(
                                pair.production_registry
                                    .is_pending_r2(ReservationId::new(101)),
                                new
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn cancel_replace_cross_model_fok_accepts_only_remaining_iceberg_at_limit() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let ids = [TradeId::new(31), TradeId::new(32), TradeId::new(33)];
    for side in [Side::Buy, Side::Sell] {
        for new in [false, true] {
            for qty in [5, 6] {
                let mut pair = Pair::new();
                pair.rest(input(1, opposite(side), 10), true, &math);
                pair.rest(input(2, opposite(side), 5), true, &math);
                pair.rest(
                    at(
                        3,
                        opposite(side),
                        if side == Side::Buy { 101 } else { 99 },
                        20,
                    ),
                    false,
                    &math,
                );
                let result = pair.execute(&request(new, side, qty, TimeInForce::Fok), &math, &ids);
                if qty == 6 {
                    assert_eq!(
                        result.unwrap_err().reason,
                        RejectReason::FokInsufficientLiquidity
                    );
                    assert_eq!(pair.production.next_queue_priority(), 3);
                } else {
                    let mut golden = vec![canceled(1, 10, CancelReason::CancelReplace)];
                    for (index, fill) in [2, 2, 1].into_iter().enumerate() {
                        golden.push(EngineEvent::Trade(TradeEvent {
                            trade_id: ids[index],
                            maker_order_id: OrderId::new(2),
                            taker_order_id: OrderId::new(9),
                            price: Price::try_new(100).unwrap(),
                            qty: Qty::try_new(fill).unwrap(),
                            maker_side: opposite(side),
                        }));
                        if index < 2 {
                            golden.push(EngineEvent::OrderPartiallyFilled {
                                order_id: OrderId::new(2),
                                remaining: Qty::try_new(5 - 2 * (index as u64 + 1)).unwrap(),
                            });
                            golden.push(EngineEvent::IcebergRefreshed {
                                order_id: OrderId::new(2),
                                visible: Qty::try_new(if index == 0 { 2 } else { 1 }).unwrap(),
                            });
                        } else {
                            golden.push(EngineEvent::OrderFilled {
                                order_id: OrderId::new(2),
                            });
                        }
                    }
                    assert_eq!(result.unwrap(), golden);
                    assert_eq!(pair.production.next_queue_priority(), 5);
                    assert_eq!(
                        pair.production
                            .order(OrderId::new(3))
                            .unwrap()
                            .total_remaining(),
                        20
                    );
                    assert!(pair.production.order(OrderId::new(9)).is_none());
                    assert!(
                        pair.production_registry
                            .is_pending_r2(ReservationId::new(102))
                    );
                    assert!(
                        pair.production_registry
                            .is_pending_r2(ReservationId::new(if new { 109 } else { 101 }))
                    );
                }
            }
        }
    }
}

#[test]
fn cancel_replace_cross_model_ioc_stops_at_real_limit_without_market_protection_context() {
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    for side in [Side::Buy, Side::Sell] {
        for new in [false, true] {
            let mut pair = Pair::new();
            pair.rest(
                at(
                    1,
                    opposite(side),
                    if side == Side::Buy { 99 } else { 101 },
                    10,
                ),
                true,
                &math,
            );
            pair.rest(input(2, opposite(side), 2), false, &math);
            pair.rest(
                at(
                    3,
                    opposite(side),
                    if side == Side::Buy { 101 } else { 99 },
                    3,
                ),
                false,
                &math,
            );
            assert_eq!(
                pair.execute(
                    &request(new, side, 4, TimeInForce::Ioc),
                    &math,
                    &[TradeId::new(41)]
                )
                .unwrap(),
                vec![
                    canceled(1, 10, CancelReason::CancelReplace),
                    EngineEvent::Trade(TradeEvent {
                        trade_id: TradeId::new(41),
                        maker_order_id: OrderId::new(2),
                        taker_order_id: OrderId::new(9),
                        price: Price::try_new(100).unwrap(),
                        qty: Qty::try_new(2).unwrap(),
                        maker_side: opposite(side)
                    }),
                    canceled(9, 2, CancelReason::IocRemainder),
                ]
            );
            assert_eq!(
                pair.production
                    .order(OrderId::new(3))
                    .unwrap()
                    .total_remaining(),
                3
            );
            assert_eq!(pair.production.next_queue_priority(), 3);
        }
    }
}
