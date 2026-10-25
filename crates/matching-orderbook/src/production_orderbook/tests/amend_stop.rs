//! 未触发止损改单的双模型固定矩阵、真实索引与失败原子性验证。

use super::*;
use crate::ReferenceOrderBook;
use matching_domain::{
    AmendOrder, AmendedQuantity, AssetId, AssetReserve, LogicalTime, MarketId,
    MarketProtectionPatch, PriceReferenceKind, QuoteBudget, ReservationId, StopLimitOrder,
    StopMarketOrder, StopMarketQuantity, StopOrder, StopPriority, TimeInForce, TriggerExpiry,
    TriggerExpiryPatch,
};
use matching_risk::ReservationState;

fn stop(id: u128, side: Side, trigger: i64, expiry: TriggerExpiry) -> StopOrder {
    StopOrder::Limit(StopLimitOrder {
        trigger_reference: matching_domain::PriceReferenceKind::LastTrade,
        order_id: OrderId::new(id),
        user_id: UserId::new(4),
        reservation_id: ReservationId::new(id),
        fee_schedule_version: 2,
        side,
        stop_price: Price::try_new(trigger).unwrap(),
        price: Price::try_new(101).unwrap(),
        qty: Qty::try_new(8).unwrap(),
        child_tif: TimeInForce::Gtc,
        trigger_expiry: expiry,
    })
}

fn shadow(id: OrderId) -> ReservationState {
    ReservationState {
        reservation_id: ReservationId::new(id.get()),
        order_id: id,
        user_id: UserId::new(4),
        market_id: MarketId::new(7),
        version: 11,
        reserves: vec![AssetReserve {
            asset: AssetId::new(2),
            atoms: 9000,
        }],
    }
}

fn amend(id: u128) -> AmendOrder {
    AmendOrder {
        market_id: MarketId::new(7),
        user_id: UserId::new(4),
        order_id: OrderId::new(id),
        new_price: None,
        new_qty: None,
        new_quote_budget: None,
        new_visible_qty: None,
        new_stop_price: None,
        new_trigger_reference: None,
        new_trigger_expiry: None,
        new_tif: None,
        new_market_protection: None,
        reservation_adjustment: None,
    }
}

pub(super) fn production_snapshot(book: &ProductionOrderBook) -> String {
    let mut ids = book.orders.iter().collect::<Vec<_>>();
    ids.sort_unstable_by_key(|(id, _)| **id);
    let mut shadows = book.reservations.iter().collect::<Vec<_>>();
    shadows.sort_unstable_by_key(|(id, _)| **id);
    format!(
        "{:?}",
        (
            level_snapshot(&book.bids),
            level_snapshot(&book.asks),
            book.arena.test_snapshot(),
            ids,
            shadows,
            book.stop_book.test_deterministic_state(),
            &book.regular_expiry,
            &book.queue_priority_allocator,
            &book.stop_priority_allocator,
            &book.reference_prices
        )
    )
}

struct Pair {
    reference: ReferenceOrderBook,
    production: ProductionOrderBook,
}

impl Pair {
    fn new() -> Self {
        Self {
            reference: ReferenceOrderBook::new(),
            production: ProductionOrderBook::new(policy()),
        }
    }
    fn place(&mut self, input: StopOrder) {
        let state = shadow(input.order_id());
        assert_eq!(
            self.reference.place_stop_with_reservation(
                input.clone(),
                LogicalTime::new(1),
                9,
                state.clone()
            ),
            self.production
                .place_stop_with_reservation(input, LogicalTime::new(1), 9, state)
        );
        self.validate();
    }
    fn validate(&self) {
        assert_eq!(validate_book(&self.production), Ok(()));
        assert_eq!(self.production.validate_stop_book(), Ok(()));
        assert_eq!(self.production.validate_regular_expiry(), Ok(()));
        for side in [Side::Buy, Side::Sell] {
            assert_eq!(
                self.reference.stop_orders(side),
                self.production.stop_orders(side)
            );
            for (input, _) in self.production.stop_orders(side) {
                assert_eq!(
                    self.reference.reservation_state(input.order_id()),
                    Some(&shadow(input.order_id()))
                );
                assert_eq!(
                    self.production.reservation_state(input.order_id()),
                    Some(&shadow(input.order_id()))
                );
            }
        }
        assert_eq!(
            self.reference.next_stop_priority(),
            self.production.next_stop_priority()
        );
        assert_eq!(
            self.reference.test_deterministic_state().1,
            self.production.next_queue_priority()
        );
    }
    fn run(&mut self, patch: &AmendOrder) -> Result<Vec<EngineEvent>, RejectReason> {
        self.validate();
        let reference_before = self.reference.test_deterministic_state();
        let production_before = production_snapshot(&self.production);
        let expected = self
            .reference
            .amend_stop_with_events(patch, LogicalTime::new(10), 9);
        let actual = self
            .production
            .amend_stop_with_events(patch, LogicalTime::new(10), 9);
        assert_eq!(actual, expected);
        if actual.is_err() {
            assert_eq!(self.reference.test_deterministic_state(), reference_before);
            assert_eq!(production_snapshot(&self.production), production_before);
        }
        self.validate();
        actual
    }
}

#[test]
fn amend_stop_child_quantity_price_and_tif_keep_trigger_fifo() {
    for side in [Side::Buy, Side::Sell] {
        let mut pair = Pair::new();
        for id in [8, 2, 6] {
            pair.place(stop(id, side, 100, TriggerExpiry::Gtc));
        }
        for qty in [5, 12] {
            let mut patch = amend(2);
            patch.new_qty = Some(Qty::try_new(qty).unwrap());
            patch.new_price = Some(Price::try_new(109).unwrap());
            patch.new_tif = Some(TimeInForce::Gtd {
                expire_at: LogicalTime::new(40),
            });
            assert_eq!(
                pair.run(&patch).unwrap(),
                vec![EngineEvent::OrderAmended {
                    order_id: OrderId::new(2),
                    old_price: Some(Price::try_new(if qty == 5 { 101 } else { 109 }).unwrap()),
                    new_price: Some(Price::try_new(109).unwrap()),
                    old_quantity: AmendedQuantity::BaseRemaining(
                        Qty::try_new(if qty == 5 { 8 } else { 5 }).unwrap()
                    ),
                    new_quantity: AmendedQuantity::BaseRemaining(Qty::try_new(qty).unwrap()),
                    priority_retained: true,
                }]
            );
            assert_eq!(
                pair.production
                    .stop_orders(side)
                    .iter()
                    .map(|(o, p)| (o.order_id().get(), p.get()))
                    .collect::<Vec<_>>(),
                vec![(8, 0), (2, 1), (6, 2)]
            );
            let StopOrder::Limit(updated) = pair.production.stop_order(OrderId::new(2)).unwrap().0
            else {
                panic!()
            };
            assert_eq!(
                updated.child_tif,
                TimeInForce::Gtd {
                    expire_at: LogicalTime::new(40)
                }
            );
            assert_eq!(updated.qty.get(), qty);
            assert_eq!(pair.production.next_queue_priority(), 0);
        }
    }
}

#[test]
fn amend_stop_trigger_price_moves_to_existing_target_tail_once() {
    for side in [Side::Buy, Side::Sell] {
        let mut pair = Pair::new();
        pair.place(stop(
            8,
            side,
            100,
            TriggerExpiry::Gtd {
                expire_at: LogicalTime::new(20),
            },
        ));
        pair.place(stop(2, side, 110, TriggerExpiry::Gtc));
        pair.place(stop(6, side, 110, TriggerExpiry::Gtc));
        let mut patch = amend(8);
        patch.new_stop_price = Some(Price::try_new(110).unwrap());
        patch.new_trigger_expiry = Some(TriggerExpiryPatch::Set(TriggerExpiry::Gtd {
            expire_at: LogicalTime::new(30),
        }));
        assert!(matches!(
            pair.run(&patch).unwrap().as_slice(),
            [EngineEvent::OrderAmended {
                priority_retained: false,
                ..
            }]
        ));
        assert_eq!(
            pair.production
                .stop_orders(side)
                .iter()
                .map(|(o, p)| (o.order_id().get(), p.get()))
                .collect::<Vec<_>>(),
            vec![(2, 1), (6, 2), (8, 3)]
        );
        assert!(
            pair.production
                .expire_gtd_through(LogicalTime::new(20))
                .is_empty()
        );
        assert!(
            pair.reference
                .expire_gtd_through(LogicalTime::new(20))
                .is_empty()
        );
        assert_eq!(
            pair.production.expire_gtd_through(LogicalTime::new(30)),
            pair.reference.expire_gtd_through(LogicalTime::new(30))
        );
        assert_eq!(pair.production.reservation_state(OrderId::new(8)), None);
        pair.validate();
    }
}

#[test]
fn amend_stop_quote_budget_updates_without_mode_switch_or_regular_priority() {
    for side in [Side::Buy, Side::Sell] {
        let mut pair = Pair::new();
        pair.place(StopOrder::Market(StopMarketOrder {
            trigger_reference: matching_domain::PriceReferenceKind::LastTrade,
            market_protection: None,
            order_id: OrderId::new(1),
            user_id: UserId::new(4),
            reservation_id: ReservationId::new(1),
            fee_schedule_version: 2,
            side,
            stop_price: Price::try_new(100).unwrap(),
            quantity: StopMarketQuantity::Quote(QuoteBudget::try_new(800).unwrap()),
            child_tif: TimeInForce::Ioc,
            trigger_expiry: TriggerExpiry::Gtc,
        }));
        for (old, new) in [(800, 500), (500, 900)] {
            let mut patch = amend(1);
            patch.new_quote_budget = Some(QuoteAmount::new(new));
            assert_eq!(
                pair.run(&patch).unwrap(),
                vec![EngineEvent::OrderAmended {
                    order_id: OrderId::new(1),
                    old_price: None,
                    new_price: None,
                    old_quantity: AmendedQuantity::QuoteBudget(QuoteAmount::new(old)),
                    new_quantity: AmendedQuantity::QuoteBudget(QuoteAmount::new(new)),
                    priority_retained: true
                }]
            );
            assert_eq!(
                pair.production.stop_order(OrderId::new(1)).unwrap().1,
                StopPriority::new(0)
            );
        }
        for case in 0..4 {
            let mut patch = amend(1);
            let reason = match case {
                0 => {
                    patch.new_qty = Some(Qty::try_new(1).unwrap());
                    RejectReason::InvalidOrderCombination
                }
                1 => {
                    patch.new_quote_budget = Some(QuoteAmount::new(0));
                    RejectReason::InvalidQty
                }
                2 => {
                    patch.new_price = Some(Price::try_new(101).unwrap());
                    RejectReason::InvalidOrderCombination
                }
                _ => {
                    patch.new_tif = Some(TimeInForce::Gtc);
                    RejectReason::InvalidOrderCombination
                }
            };
            assert_eq!(pair.run(&patch), Err(reason));
        }
    }
}

#[test]
fn amend_stop_expiry_matrix_migrates_real_indices_and_clears_terminal_shadow() {
    let expiries = [
        TriggerExpiry::Gtc,
        TriggerExpiry::Gtd {
            expire_at: LogicalTime::new(20),
        },
        TriggerExpiry::Day { session_id: 9 },
    ];
    for side in [Side::Buy, Side::Sell] {
        for old in expiries {
            for target in [
                TriggerExpiry::Gtc,
                TriggerExpiry::Gtd {
                    expire_at: LogicalTime::new(30),
                },
                TriggerExpiry::Day { session_id: 9 },
            ] {
                let mut pair = Pair::new();
                pair.place(stop(8, side, 100, old));
                pair.place(stop(2, side, 100, TriggerExpiry::Gtc));
                let mut patch = amend(8);
                patch.new_trigger_expiry = Some(if target == TriggerExpiry::Gtc {
                    TriggerExpiryPatch::ClearToGtc
                } else {
                    TriggerExpiryPatch::Set(target)
                });
                assert!(matches!(
                    pair.run(&patch).unwrap().as_slice(),
                    [EngineEvent::OrderAmended {
                        priority_retained: true,
                        ..
                    }]
                ));
                assert_eq!(
                    pair.production.stop_order(OrderId::new(8)).unwrap().1,
                    StopPriority::new(0)
                );
                assert_eq!(pair.production.next_stop_priority(), 2);
                assert!(
                    pair.production
                        .expire_gtd_through(LogicalTime::new(20))
                        .is_empty()
                );
                assert!(
                    pair.reference
                        .expire_gtd_through(LogicalTime::new(20))
                        .is_empty()
                );
                assert_eq!(
                    pair.production.expire_gtd_through(LogicalTime::new(30)),
                    pair.reference.expire_gtd_through(LogicalTime::new(30))
                );
                assert_eq!(
                    pair.production.end_session(9, 9),
                    pair.reference.end_session(9, 9)
                );
                if target == TriggerExpiry::Gtc {
                    assert_eq!(
                        pair.production
                            .stop_order(OrderId::new(8))
                            .unwrap()
                            .0
                            .trigger_expiry(),
                        target
                    );
                } else {
                    assert!(pair.production.stop_order(OrderId::new(8)).is_none());
                    assert!(pair.reference.reservation_state(OrderId::new(8)).is_none());
                    assert!(pair.production.reservation_state(OrderId::new(8)).is_none());
                }
                pair.validate();
            }
        }
    }
}

#[test]
fn amend_stop_patch_validation_and_failure_atomicity() {
    let mut pair = Pair::new();
    pair.place(stop(
        1,
        Side::Buy,
        100,
        TriggerExpiry::Gtd {
            expire_at: LogicalTime::new(20),
        },
    ));
    pair.place(stop(
        2,
        Side::Sell,
        95,
        TriggerExpiry::Day { session_id: 9 },
    ));
    for case in 0..15 {
        let mut patch = amend(1);
        // 同时要求迁移触发档位与到期引用，确保拒绝不会留下部分修改。
        patch.new_stop_price = Some(Price::try_new(110).unwrap());
        patch.new_trigger_expiry = Some(TriggerExpiryPatch::ClearToGtc);
        let reason = match case {
            0 => {
                patch.new_trigger_reference = Some(PriceReferenceKind::StaticOpening);
                RejectReason::InvalidReferencePriceSource
            }
            1 => {
                patch.new_market_protection = Some(MarketProtectionPatch::Keep);
                assert!(pair.run(&patch).is_ok());
                continue;
            }
            2 => {
                patch.new_market_protection = Some(MarketProtectionPatch::Clear);
                assert!(pair.run(&patch).is_ok());
                continue;
            }
            3 => {
                patch.new_visible_qty = Some(Qty::try_new(1).unwrap());
                RejectReason::InvalidOrderCombination
            }
            4 => {
                patch.new_quote_budget = Some(QuoteAmount::new(10));
                RejectReason::InvalidOrderCombination
            }
            5 => {
                patch.new_trigger_expiry = Some(TriggerExpiryPatch::Set(TriggerExpiry::Gtd {
                    expire_at: LogicalTime::new(10),
                }));
                RejectReason::InvalidExpireTime
            }
            6 => {
                patch.new_trigger_expiry = Some(TriggerExpiryPatch::Set(TriggerExpiry::Day {
                    session_id: 8,
                }));
                RejectReason::InvalidExpireTime
            }
            7 => {
                patch.new_tif = Some(TimeInForce::Gtd {
                    expire_at: LogicalTime::new(10),
                });
                RejectReason::InvalidExpireTime
            }
            8 => {
                patch.new_tif = Some(TimeInForce::Day { session_id: 8 });
                RejectReason::InvalidSession
            }
            9 => {
                patch.new_tif = Some(TimeInForce::Ioc);
                RejectReason::InvalidOrderCombination
            }
            10 => {
                patch.new_tif = Some(TimeInForce::Fok);
                RejectReason::InvalidOrderCombination
            }
            11 => {
                patch.user_id = UserId::new(5);
                RejectReason::InvalidReservation
            }
            12 => {
                patch.market_id = MarketId::new(8);
                RejectReason::InvalidReservation
            }
            13 => {
                patch.new_market_protection = Some(MarketProtectionPatch::Set(
                    matching_domain::MarketProtection {
                        max_slippage_bps: Some(100),
                        worst_price: None,
                    },
                ));
                RejectReason::InvalidOrderCombination
            }
            _ => {
                patch.reservation_adjustment = Some(matching_domain::ReservationAdjustmentToken {
                    token_id: 1,
                    command_id: matching_domain::CommandId::new(1),
                    reservation_id: ReservationId::new(1),
                    order_id: patch.order_id,
                    user_id: patch.user_id,
                    market_id: patch.market_id,
                    expected_reservation_version: 11,
                    target_reserves: Default::default(),
                    fee_schedule_version: 2,
                    fee_tier_id: 0,
                    risk_key_id: 1,
                    signature: [0; 64],
                });
                RejectReason::InvalidReservation
            }
        };
        assert_eq!(pair.run(&patch), Err(reason), "case {case}");
    }
    // 遗留接收入口不生成预留影子状态；改单不能替它编造默认值。
    let legacy = stop(3, Side::Buy, 90, TriggerExpiry::Gtc);
    pair.reference
        .place_stop(legacy.clone(), LogicalTime::new(1), 9)
        .unwrap();
    pair.production
        .place_stop(legacy, LogicalTime::new(1), 9)
        .unwrap();
    let before_ref = pair.reference.test_deterministic_state();
    let before_prod = production_snapshot(&pair.production);
    assert_eq!(
        pair.reference
            .amend_stop_with_events(&amend(3), LogicalTime::new(10), 9),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(
        pair.production
            .amend_stop_with_events(&amend(3), LogicalTime::new(10), 9),
        Err(RejectReason::InvalidReservation)
    );
    assert_eq!(pair.reference.test_deterministic_state(), before_ref);
    assert_eq!(production_snapshot(&pair.production), before_prod);
}

#[test]
fn amend_stop_day_aggregate_expiry_cleans_shadow_after_successful_migration() {
    let mut pair = Pair::new();
    pair.place(stop(1, Side::Sell, 100, TriggerExpiry::Gtc));
    let mut patch = amend(1);
    patch.new_trigger_expiry = Some(TriggerExpiryPatch::Set(TriggerExpiry::Day {
        session_id: 9,
    }));
    pair.run(&patch).unwrap();
    assert_eq!(
        pair.production.end_session(9, 9),
        pair.reference.end_session(9, 9)
    );
    assert!(pair.production.reservation_state(OrderId::new(1)).is_none());
    assert!(pair.reference.reservation_state(OrderId::new(1)).is_none());
    pair.validate();
}

#[test]
fn amend_stop_production_plan_is_read_only_and_exhaustion_only_blocks_requeue() {
    let mut book = ProductionOrderBook::new(policy());
    let input = stop(
        1,
        Side::Buy,
        100,
        TriggerExpiry::Gtd {
            expire_at: LogicalTime::new(20),
        },
    );
    book.place_stop_with_reservation(
        input.clone(),
        LogicalTime::new(1),
        9,
        shadow(input.order_id()),
    )
    .unwrap();
    let mut replacement = input;
    let StopOrder::Limit(child) = &mut replacement else {
        panic!()
    };
    child.stop_price = Price::try_new(110).unwrap();
    child.trigger_expiry = TriggerExpiry::Day { session_id: 9 };
    let before = production_snapshot(&book);
    let _plan = book
        .stop_book
        .plan_replacement(
            replacement,
            LogicalTime::new(10),
            9,
            &book.stop_priority_allocator,
        )
        .unwrap();
    assert_eq!(production_snapshot(&book), before);
    book.stop_priority_allocator = crate::StopPriorityAllocator::try_restore(
        QueuePriorityPolicy::try_with_synthetic_ceiling(1, Some(1)).unwrap(),
        1,
    )
    .unwrap();
    let mut patch = amend(1);
    patch.new_stop_price = Some(Price::try_new(110).unwrap());
    patch.new_trigger_expiry = Some(TriggerExpiryPatch::ClearToGtc);
    let before = production_snapshot(&book);
    assert_eq!(
        book.amend_stop_with_events(&patch, LogicalTime::new(10), 9),
        Err(RejectReason::PrioritySpaceExhaustion)
    );
    assert_eq!(production_snapshot(&book), before);
    patch.new_stop_price = Some(Price::try_new(100).unwrap());
    patch.new_trigger_reference = Some(PriceReferenceKind::MarkPrice);
    assert_eq!(
        book.amend_stop_with_events(&patch, LogicalTime::new(10), 9),
        Err(RejectReason::PrioritySpaceExhaustion)
    );
    assert_eq!(production_snapshot(&book), before);
    patch.new_trigger_reference = None;
    patch.new_stop_price = Some(Price::try_new(100).unwrap());
    patch.new_qty = Some(Qty::try_new(10).unwrap());
    assert!(matches!(
        book.amend_stop_with_events(&patch, LogicalTime::new(10), 9)
            .unwrap()
            .as_slice(),
        [EngineEvent::OrderAmended {
            priority_retained: true,
            ..
        }]
    ));
    assert_eq!(book.next_stop_priority(), 1);
    assert_eq!(book.next_queue_priority(), 0);
    assert!(book.expire_gtd_through(LogicalTime::new(20)).is_empty());
    assert_eq!(book.validate_stop_book(), Ok(()));
}

#[test]
fn amend_stop_base_market_updates_child_and_rejects_budget_or_price_mode_switch() {
    for side in [Side::Buy, Side::Sell] {
        let mut pair = Pair::new();
        pair.place(StopOrder::Market(StopMarketOrder {
            trigger_reference: matching_domain::PriceReferenceKind::LastTrade,
            market_protection: None,
            order_id: OrderId::new(1),
            user_id: UserId::new(4),
            reservation_id: ReservationId::new(1),
            fee_schedule_version: 2,
            side,
            stop_price: Price::try_new(100).unwrap(),
            quantity: StopMarketQuantity::Base(Qty::try_new(8).unwrap()),
            child_tif: TimeInForce::Ioc,
            trigger_expiry: TriggerExpiry::Gtc,
        }));
        let mut patch = amend(1);
        patch.new_qty = Some(Qty::try_new(12).unwrap());
        assert_eq!(
            pair.run(&patch).unwrap(),
            vec![EngineEvent::OrderAmended {
                order_id: OrderId::new(1),
                old_price: None,
                new_price: None,
                old_quantity: AmendedQuantity::BaseRemaining(Qty::try_new(8).unwrap()),
                new_quantity: AmendedQuantity::BaseRemaining(Qty::try_new(12).unwrap()),
                priority_retained: true
            }]
        );
        patch.new_quote_budget = Some(QuoteAmount::new(10));
        assert_eq!(pair.run(&patch), Err(RejectReason::InvalidOrderCombination));
        patch.new_quote_budget = None;
        patch.new_price = Some(Price::try_new(101).unwrap());
        assert_eq!(pair.run(&patch), Err(RejectReason::InvalidOrderCombination));
    }
}

#[test]
fn amend_stop_trigger_consumes_updated_template_then_allocates_regular_priority() {
    for side in [Side::Buy, Side::Sell] {
        let mut pair = Pair::new();
        pair.place(stop(
            1,
            side,
            100,
            TriggerExpiry::Gtd {
                expire_at: LogicalTime::new(20),
            },
        ));
        let maker_side = if side == Side::Buy {
            Side::Sell
        } else {
            Side::Buy
        };
        let maker = order(9, maker_side, 100, 2);
        pair.reference.rest(maker.clone()).unwrap();
        pair.production.rest(maker).unwrap();
        let mut patch = amend(1);
        patch.new_qty = Some(Qty::try_new(5).unwrap());
        patch.new_price = Some(Price::try_new(100).unwrap());
        patch.new_stop_price =
            Some(Price::try_new(if side == Side::Buy { 105 } else { 95 }).unwrap());
        patch.new_trigger_expiry = Some(TriggerExpiryPatch::Keep);
        pair.run(&patch).unwrap();
        let expected = pair
            .production
            .stop_order(OrderId::new(1))
            .unwrap()
            .0
            .clone();
        let initial = matching_domain::PriceReference {
            kind: PriceReferenceKind::LastTrade,
            source_seq: 1,
            price: Price::try_new(if side == Side::Buy { 90 } else { 110 }).unwrap(),
        };
        assert_eq!(
            pair.production.collect_triggered_stops(initial),
            pair.reference.collect_triggered_stops(initial)
        );
        let update = matching_domain::PriceReference {
            source_seq: 2,
            price: patch.new_stop_price.unwrap(),
            ..initial
        };
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let reference_events = pair
            .reference
            .trigger_stops(
                update,
                LogicalTime::new(10),
                9,
                &math,
                &[TradeId::new(1)],
                |child| {
                    assert_eq!(child, &expected);
                    Ok(())
                },
            )
            .unwrap();
        let events = pair
            .production
            .trigger_stops(
                update,
                LogicalTime::new(10),
                9,
                &math,
                &[TradeId::new(1)],
                |child| {
                    assert_eq!(child, &expected);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(events, reference_events);
        assert!(
            matches!(events.first(), Some(EngineEvent::StopTriggered {order_id, ..}) if *order_id == OrderId::new(1))
        );
        assert!(
            events
                .iter()
                .any(|event| matches!(event, EngineEvent::Trade(trade) if trade.qty.get() == 2))
        );
        assert_eq!(
            pair.production.order(OrderId::new(1)).unwrap().remaining(),
            3
        );
        assert_eq!(
            pair.production
                .order(OrderId::new(1))
                .unwrap()
                .priority()
                .get(),
            1
        );
        assert_eq!(pair.production.next_stop_priority(), 2);
        assert_eq!(pair.production.next_queue_priority(), 2);
        assert!(
            pair.production
                .expire_stop_gtd_through(LogicalTime::new(20))
                .is_empty()
        );
        assert!(
            pair.reference
                .expire_stop_gtd_through(LogicalTime::new(20))
                .is_empty()
        );
        pair.validate();
    }
}

#[test]
fn amend_stop_child_tif_matrix_keeps_trigger_lifetime_and_queue_independent() {
    let tifs = [
        TimeInForce::Gtc,
        TimeInForce::Day { session_id: 9 },
        TimeInForce::Gtd {
            expire_at: LogicalTime::new(40),
        },
    ];
    for side in [Side::Buy, Side::Sell] {
        for old in tifs {
            for target in tifs {
                let mut pair = Pair::new();
                let mut input = stop(
                    1,
                    side,
                    100,
                    TriggerExpiry::Gtd {
                        expire_at: LogicalTime::new(20),
                    },
                );
                let StopOrder::Limit(child) = &mut input else {
                    panic!()
                };
                child.child_tif = old;
                pair.place(input);
                let mut patch = amend(1);
                patch.new_tif = Some(target);
                patch.new_stop_price = Some(Price::try_new(100).unwrap());
                assert!(matches!(
                    pair.run(&patch).unwrap().as_slice(),
                    [EngineEvent::OrderAmended {
                        priority_retained: true,
                        ..
                    }]
                ));
                let StopOrder::Limit(child) =
                    pair.production.stop_order(OrderId::new(1)).unwrap().0
                else {
                    panic!()
                };
                assert_eq!(child.child_tif, target);
                assert_eq!(
                    child.trigger_expiry,
                    TriggerExpiry::Gtd {
                        expire_at: LogicalTime::new(20)
                    }
                );
                assert_eq!(pair.production.next_stop_priority(), 1);
                assert_eq!(pair.production.next_queue_priority(), 0);
                assert_eq!(
                    pair.production.expire_gtd_through(LogicalTime::new(20)),
                    pair.reference.expire_gtd_through(LogicalTime::new(20))
                );
                assert!(pair.production.stop_order(OrderId::new(1)).is_none());
                assert!(pair.production.reservation_state(OrderId::new(1)).is_none());
                pair.validate();
            }
        }
    }
}

#[test]
fn amend_stop_reference_changes_requeue_at_target_source_tail() {
    for side in [Side::Buy, Side::Sell] {
        for target_source in [
            PriceReferenceKind::MarkPrice,
            PriceReferenceKind::IndexPrice,
        ] {
            let mut pair = Pair::new();
            pair.place(stop(
                1,
                side,
                100,
                TriggerExpiry::Gtd {
                    expire_at: LogicalTime::new(20),
                },
            ));
            pair.place(stop(2, side, 100, TriggerExpiry::Gtc));
            let mut patch = amend(2);
            patch.new_trigger_reference = Some(target_source);
            assert!(matches!(
                pair.run(&patch).unwrap().as_slice(),
                [EngineEvent::OrderAmended {
                    priority_retained: false,
                    ..
                }]
            ));
            patch.order_id = OrderId::new(1);
            pair.run(&patch).unwrap();
            assert_eq!(pair.production.next_stop_priority(), 4);
            assert!(matches!(
                pair.run(&patch).unwrap().as_slice(),
                [EngineEvent::OrderAmended {
                    priority_retained: true,
                    ..
                }]
            ));
            assert_eq!(pair.production.next_stop_priority(), 4);
            let initial = matching_domain::PriceReference {
                kind: PriceReferenceKind::LastTrade,
                source_seq: 1,
                price: Price::try_new(if side == Side::Buy { 90 } else { 110 }).unwrap(),
            };
            pair.production.collect_triggered_stops(initial).unwrap();
            pair.reference.collect_triggered_stops(initial).unwrap();
            let update = matching_domain::PriceReference {
                source_seq: 2,
                price: Price::try_new(if side == Side::Buy { 110 } else { 90 }).unwrap(),
                ..initial
            };
            assert!(
                pair.production
                    .collect_triggered_stops(update)
                    .unwrap()
                    .is_empty()
            );
            assert!(
                pair.reference
                    .collect_triggered_stops(update)
                    .unwrap()
                    .is_empty()
            );
            let initial = matching_domain::PriceReference {
                kind: target_source,
                ..initial
            };
            pair.production.collect_triggered_stops(initial).unwrap();
            pair.reference.collect_triggered_stops(initial).unwrap();
            let update = matching_domain::PriceReference {
                kind: target_source,
                ..update
            };
            let actual = pair.production.collect_triggered_stops(update).unwrap();
            assert_eq!(
                actual,
                pair.reference.collect_triggered_stops(update).unwrap()
            );
            assert_eq!(
                actual
                    .iter()
                    .map(|(order, priority, _, _)| (order.order_id().get(), priority.get()))
                    .collect::<Vec<_>>(),
                vec![(2, 2), (1, 3)]
            );
            pair.validate();
            assert!(
                pair.production
                    .expire_stop_gtd_through(LogicalTime::new(20))
                    .is_empty()
            );
            assert!(
                pair.reference
                    .expire_stop_gtd_through(LogicalTime::new(20))
                    .is_empty()
            );
        }
    }
}

#[test]
fn amend_stop_market_protection_keep_clear_and_set_preserve_trigger_priority() {
    let mut pair = Pair::new();
    pair.place(StopOrder::Market(StopMarketOrder {
        trigger_reference: matching_domain::PriceReferenceKind::LastTrade,
        market_protection: None,
        order_id: OrderId::new(1),
        user_id: UserId::new(4),
        reservation_id: ReservationId::new(1),
        fee_schedule_version: 2,
        side: Side::Buy,
        stop_price: Price::try_new(100).unwrap(),
        quantity: StopMarketQuantity::Base(Qty::try_new(8).unwrap()),
        child_tif: TimeInForce::Ioc,
        trigger_expiry: TriggerExpiry::Gtc,
    }));
    for protection in [
        MarketProtectionPatch::Set(matching_domain::MarketProtection {
            max_slippage_bps: Some(20),
            worst_price: None,
        }),
        MarketProtectionPatch::Keep,
        MarketProtectionPatch::Clear,
    ] {
        let mut patch = amend(1);
        let cleared = matches!(protection, MarketProtectionPatch::Clear);
        patch.new_market_protection = Some(protection);
        assert!(matches!(
            pair.run(&patch).unwrap().as_slice(),
            [EngineEvent::OrderAmended {
                priority_retained: true,
                ..
            }]
        ));
        assert_eq!(
            pair.production.stop_order(OrderId::new(1)).unwrap().1.get(),
            0
        );
        assert_eq!(pair.production.next_stop_priority(), 1);
        let expected = if cleared {
            None
        } else {
            Some(matching_domain::MarketProtection {
                max_slippage_bps: Some(20),
                worst_price: None,
            })
        };
        assert_eq!(
            pair.production
                .stop_order(OrderId::new(1))
                .unwrap()
                .0
                .market_protection(),
            expected.as_ref()
        );
        let candidate = pair
            .production
            .build_amend_plan(&amend(1), LogicalTime::new(10), 9)
            .unwrap();
        assert!(
            matches!(candidate,super::super::amend_candidate::ProductionAmendPlan::Stop(plan) if plan.old_protection==expected && plan.new_protection==expected && plan.old_reference==PriceReferenceKind::LastTrade && plan.new_reference==PriceReferenceKind::LastTrade)
        );
    }
}

#[test]
fn amend_stop_timed_child_executes_then_regular_residual_expires() {
    for tif in [
        TimeInForce::Day { session_id: 9 },
        TimeInForce::Gtd {
            expire_at: LogicalTime::new(40),
        },
    ] {
        let mut pair = Pair::new();
        pair.place(stop(1, Side::Buy, 100, TriggerExpiry::Gtc));
        let mut patch = amend(1);
        patch.new_tif = Some(tif);
        pair.run(&patch).unwrap();
        let maker = order(9, Side::Sell, 100, 2);
        pair.production.rest(maker.clone()).unwrap();
        pair.reference.rest(maker).unwrap();
        let initial = matching_domain::PriceReference {
            kind: PriceReferenceKind::LastTrade,
            source_seq: 1,
            price: Price::try_new(90).unwrap(),
        };
        pair.production.collect_triggered_stops(initial).unwrap();
        pair.reference.collect_triggered_stops(initial).unwrap();
        let update = matching_domain::PriceReference {
            source_seq: 2,
            price: Price::try_new(100).unwrap(),
            ..initial
        };
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let mut calls = 0;
        let result = pair
            .production
            .trigger_stops(
                update,
                LogicalTime::new(10),
                9,
                &math,
                &[TradeId::new(1)],
                |_| {
                    calls += 1;
                    Ok(())
                },
            )
            .unwrap();
        let expected = pair
            .reference
            .trigger_stops(
                update,
                LogicalTime::new(10),
                9,
                &math,
                &[TradeId::new(1)],
                |_| Ok(()),
            )
            .unwrap();
        assert!(
            result
                .iter()
                .any(|e| matches!(e,EngineEvent::Trade(trade) if trade.qty.get()==2))
        );
        assert_eq!(
            pair.production.order(OrderId::new(1)).unwrap().remaining(),
            6
        );
        assert_eq!(calls, 1);
        assert_eq!(result, expected);
        assert!(
            !result
                .iter()
                .any(|e| matches!(e, EngineEvent::TriggeredOrderRejected { .. }))
        );
        assert_eq!(
            pair.production
                .order(OrderId::new(1))
                .unwrap()
                .time_in_force(),
            tif
        );
        assert_eq!(pair.production.validate_regular_expiry(), Ok(()));
        assert_eq!(
            pair.production.expire_gtd_through(LogicalTime::new(40)),
            pair.reference.expire_gtd_through(LogicalTime::new(40))
        );
        assert_eq!(
            pair.production.end_session(9, 9),
            pair.reference.end_session(9, 9)
        );
        assert!(pair.production.order(OrderId::new(1)).is_none());
    }
}

#[test]
fn amend_stop_protection_is_enforced_when_market_child_executes() {
    for source in [
        PriceReferenceKind::LastTrade,
        PriceReferenceKind::MarkPrice,
        PriceReferenceKind::IndexPrice,
    ] {
        for quote in [false, true] {
            let mut pair = Pair::new();
            pair.place(StopOrder::Market(StopMarketOrder {
                trigger_reference: source,
                market_protection: None,
                order_id: OrderId::new(1),
                user_id: UserId::new(4),
                reservation_id: ReservationId::new(1),
                fee_schedule_version: 2,
                side: Side::Buy,
                stop_price: Price::try_new(100).unwrap(),
                quantity: if quote {
                    StopMarketQuantity::Quote(QuoteBudget::try_new(2000).unwrap())
                } else {
                    StopMarketQuantity::Base(Qty::try_new(8).unwrap())
                },
                child_tif: TimeInForce::Ioc,
                trigger_expiry: TriggerExpiry::Gtc,
            }));
            let mut patch = amend(1);
            patch.new_market_protection = Some(MarketProtectionPatch::Set(
                matching_domain::MarketProtection {
                    max_slippage_bps: Some(0),
                    worst_price: None,
                },
            ));
            pair.run(&patch).unwrap();
            let maker = order(9, Side::Sell, 101, 8);
            pair.production.rest(maker.clone()).unwrap();
            pair.reference.rest(maker).unwrap();
            let initial = matching_domain::PriceReference {
                kind: source,
                source_seq: 1,
                price: Price::try_new(90).unwrap(),
            };
            pair.production.collect_triggered_stops(initial).unwrap();
            pair.reference.collect_triggered_stops(initial).unwrap();
            let update = matching_domain::PriceReference {
                source_seq: 2,
                price: Price::try_new(100).unwrap(),
                ..initial
            };
            let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
            let actual = pair
                .production
                .trigger_stops(
                    update,
                    LogicalTime::new(10),
                    9,
                    &math,
                    &[TradeId::new(1)],
                    |_| Ok(()),
                )
                .unwrap();
            let expected = pair
                .reference
                .trigger_stops(
                    update,
                    LogicalTime::new(10),
                    9,
                    &math,
                    &[TradeId::new(1)],
                    |_| Ok(()),
                )
                .unwrap();
            assert_eq!(actual, expected);
            assert!(!actual.iter().any(|e| matches!(e, EngineEvent::Trade(_))));
            assert!(
                actual
                    .iter()
                    .any(|e| matches!(e, EngineEvent::MarketProtectionTriggered { .. }))
            );
            assert_eq!(
                pair.production.order(OrderId::new(9)).unwrap().remaining(),
                8
            );
        }
    }
}

#[test]
fn amend_stop_timed_child_rechecks_expiry_and_session_before_trigger_r1() {
    for (tif, now, session, reason) in [
        (
            TimeInForce::Day { session_id: 9 },
            LogicalTime::new(20),
            8,
            RejectReason::InvalidSession,
        ),
        (
            TimeInForce::Gtd {
                expire_at: LogicalTime::new(40),
            },
            LogicalTime::new(40),
            9,
            RejectReason::InvalidExpireTime,
        ),
    ] {
        let mut pair = Pair::new();
        pair.place(stop(1, Side::Buy, 100, TriggerExpiry::Gtc));
        let mut patch = amend(1);
        patch.new_tif = Some(tif);
        pair.run(&patch).unwrap();
        let initial = matching_domain::PriceReference {
            kind: PriceReferenceKind::LastTrade,
            source_seq: 1,
            price: Price::try_new(90).unwrap(),
        };
        pair.reference.collect_triggered_stops(initial).unwrap();
        pair.production.collect_triggered_stops(initial).unwrap();
        let update = matching_domain::PriceReference {
            source_seq: 2,
            price: Price::try_new(100).unwrap(),
            ..initial
        };
        let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
        let events = pair
            .production
            .trigger_stops(update, now, session, &math, &[], |_| {
                panic!("invalid child must not reach Trigger-R1")
            })
            .unwrap();
        let expected = pair
            .reference
            .trigger_stops(update, now, session, &math, &[], |_| {
                panic!("invalid child must not reach Trigger-R1")
            })
            .unwrap();
        assert_eq!(events, expected);
        assert!(
            matches!(events.as_slice(),[EngineEvent::StopTriggered {..},EngineEvent::TriggeredOrderRejected {reason:actual,..},EngineEvent::ReservationReleased {..}] if *actual==reason)
        );
        assert!(pair.production.stop_order(OrderId::new(1)).is_none());
        assert!(pair.production.order(OrderId::new(1)).is_none());
        assert!(pair.production.reservation_state(OrderId::new(1)).is_none());
        assert_eq!(pair.production.next_queue_priority(), 0);
        pair.validate();
    }
}

#[test]
fn amend_stop_invalid_source_and_protection_reject_at_placement_and_amend() {
    let mut pair = Pair::new();
    let mut market = StopOrder::Market(StopMarketOrder {
        trigger_reference: PriceReferenceKind::MarkPrice,
        market_protection: Some(matching_domain::MarketProtection {
            max_slippage_bps: Some(50),
            worst_price: None,
        }),
        order_id: OrderId::new(1),
        user_id: UserId::new(4),
        reservation_id: ReservationId::new(1),
        fee_schedule_version: 2,
        side: Side::Buy,
        stop_price: Price::try_new(100).unwrap(),
        quantity: StopMarketQuantity::Base(Qty::try_new(8).unwrap()),
        child_tif: TimeInForce::Ioc,
        trigger_expiry: TriggerExpiry::Gtd {
            expire_at: LogicalTime::new(20),
        },
    });
    pair.place(market.clone());
    for patch_kind in 0..2 {
        let mut patch = amend(1);
        patch.new_trigger_expiry = Some(TriggerExpiryPatch::ClearToGtc);
        patch.new_stop_price = Some(Price::try_new(110).unwrap());
        let reason = if patch_kind == 0 {
            patch.new_trigger_reference = Some(PriceReferenceKind::StaticOpening);
            RejectReason::InvalidReferencePriceSource
        } else {
            patch.new_market_protection = Some(MarketProtectionPatch::Set(
                matching_domain::MarketProtection {
                    max_slippage_bps: Some(10000),
                    worst_price: None,
                },
            ));
            RejectReason::InvalidSlippageBps
        };
        assert_eq!(pair.run(&patch), Err(reason));
    }
    let StopOrder::Market(child) = &mut market else {
        panic!()
    };
    child.order_id = OrderId::new(2);
    child.trigger_reference = PriceReferenceKind::StaticOpening;
    let before = production_snapshot(&pair.production);
    let before_ref = pair.reference.test_deterministic_state();
    assert_eq!(
        pair.production
            .place_stop(market.clone(), LogicalTime::new(1), 9),
        Err(RejectReason::InvalidReferencePriceSource)
    );
    assert_eq!(
        pair.reference
            .place_stop(market.clone(), LogicalTime::new(1), 9),
        Err(RejectReason::InvalidReferencePriceSource)
    );
    let StopOrder::Market(child) = &mut market else {
        panic!()
    };
    child.trigger_reference = PriceReferenceKind::IndexPrice;
    child.market_protection = Some(matching_domain::MarketProtection {
        max_slippage_bps: Some(10000),
        worst_price: None,
    });
    assert_eq!(
        pair.production
            .place_stop(market.clone(), LogicalTime::new(1), 9),
        Err(RejectReason::InvalidSlippageBps)
    );
    assert_eq!(
        pair.reference.place_stop(market, LogicalTime::new(1), 9),
        Err(RejectReason::InvalidSlippageBps)
    );
    assert_eq!(production_snapshot(&pair.production), before);
    assert_eq!(pair.reference.test_deterministic_state(), before_ref);
}

#[test]
fn amend_stop_timed_child_trigger_r1_rejection_prevents_any_trade_or_rest() {
    let mut pair = Pair::new();
    pair.place(stop(1, Side::Buy, 100, TriggerExpiry::Gtc));
    let mut patch = amend(1);
    patch.new_tif = Some(TimeInForce::Day { session_id: 9 });
    pair.run(&patch).unwrap();
    let initial = matching_domain::PriceReference {
        kind: PriceReferenceKind::LastTrade,
        source_seq: 1,
        price: Price::try_new(90).unwrap(),
    };
    pair.production.collect_triggered_stops(initial).unwrap();
    pair.reference.collect_triggered_stops(initial).unwrap();
    let update = matching_domain::PriceReference {
        source_seq: 2,
        price: Price::try_new(100).unwrap(),
        ..initial
    };
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let actual = pair
        .production
        .trigger_stops(update, LogicalTime::new(10), 9, &math, &[], |_| {
            Err(RejectReason::InsufficientReservation)
        })
        .unwrap();
    let expected = pair
        .reference
        .trigger_stops(update, LogicalTime::new(10), 9, &math, &[], |_| {
            Err(RejectReason::InsufficientReservation)
        })
        .unwrap();
    assert_eq!(actual, expected);
    assert!(matches!(
        actual.as_slice(),
        [
            EngineEvent::StopTriggered { .. },
            EngineEvent::TriggeredOrderRejected {
                reason: RejectReason::InsufficientReservation,
                ..
            },
            EngineEvent::ReservationReleased { .. }
        ]
    ));
    assert!(pair.production.order(OrderId::new(1)).is_none());
    assert!(pair.production.reservation_state(OrderId::new(1)).is_none());
    assert_eq!(pair.production.next_queue_priority(), 0);
    pair.validate();
}

fn root_frozen_cascade(
    side: Side,
    quote: bool,
    first_batch: bool,
    root_source: PriceReferenceKind,
) -> Vec<EngineEvent> {
    let mut pair = Pair::new();
    let (baseline, maker_price, descendant_trigger, maker_side) = if side == Side::Buy {
        (90, 110, 105, Side::Sell)
    } else {
        (110, 90, 95, Side::Buy)
    };
    let maker = order(9, maker_side, maker_price, 2);
    pair.production.rest(maker.clone()).unwrap();
    pair.reference.rest(maker).unwrap();
    let mut parent = stop(1, side, 100, TriggerExpiry::Gtc);
    let StopOrder::Limit(child) = &mut parent else {
        panic!()
    };
    child.trigger_reference = root_source;
    child.price = Price::try_new(maker_price).unwrap();
    child.qty = Qty::try_new(1).unwrap();
    pair.place(parent);
    pair.place(StopOrder::Market(StopMarketOrder {
        trigger_reference: if first_batch {
            root_source
        } else {
            PriceReferenceKind::LastTrade
        },
        market_protection: None,
        order_id: OrderId::new(2),
        user_id: UserId::new(4),
        reservation_id: ReservationId::new(2),
        fee_schedule_version: 2,
        side,
        stop_price: Price::try_new(if first_batch { 100 } else { descendant_trigger }).unwrap(),
        quantity: if quote {
            StopMarketQuantity::Quote(QuoteBudget::try_new(1000).unwrap())
        } else {
            StopMarketQuantity::Base(Qty::try_new(1).unwrap())
        },
        child_tif: TimeInForce::Ioc,
        trigger_expiry: TriggerExpiry::Gtd {
            expire_at: LogicalTime::new(20),
        },
    }));
    let mut patch = amend(2);
    patch.new_market_protection = Some(MarketProtectionPatch::Set(
        matching_domain::MarketProtection {
            max_slippage_bps: Some(0),
            worst_price: None,
        },
    ));
    pair.run(&patch).unwrap();
    let last = matching_domain::PriceReference {
        kind: PriceReferenceKind::LastTrade,
        source_seq: 1,
        price: Price::try_new(baseline).unwrap(),
    };
    pair.production.collect_triggered_stops(last).unwrap();
    pair.reference.collect_triggered_stops(last).unwrap();
    if root_source != PriceReferenceKind::LastTrade {
        let initial = matching_domain::PriceReference {
            kind: root_source,
            ..last
        };
        pair.production.collect_triggered_stops(initial).unwrap();
        pair.reference.collect_triggered_stops(initial).unwrap();
    }
    let update = matching_domain::PriceReference {
        kind: root_source,
        source_seq: 2,
        price: Price::try_new(100).unwrap(),
    };
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let ids = [TradeId::new(1), TradeId::new(2)];
    let actual = pair
        .production
        .trigger_stops(update, LogicalTime::new(10), 9, &math, &ids, |_| Ok(()))
        .unwrap();
    let expected = pair
        .reference
        .trigger_stops(update, LogicalTime::new(10), 9, &math, &ids, |_| Ok(()))
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        actual,
        vec![
            EngineEvent::StopTriggered {
                order_id: OrderId::new(1),
                child_kind: matching_domain::TriggeredOrderKind::Limit
            },
            EngineEvent::Trade(TradeEvent {
                trade_id: TradeId::new(1),
                maker_order_id: OrderId::new(9),
                taker_order_id: OrderId::new(1),
                price: Price::try_new(maker_price).unwrap(),
                qty: Qty::try_new(1).unwrap(),
                maker_side
            }),
            EngineEvent::StopTriggered {
                order_id: OrderId::new(2),
                child_kind: if quote {
                    matching_domain::TriggeredOrderKind::MarketByQuote
                } else {
                    matching_domain::TriggeredOrderKind::MarketByBase
                }
            },
            EngineEvent::MarketProtectionTriggered {
                order_id: OrderId::new(2),
                next_price: Price::try_new(maker_price).unwrap(),
                effective_worst_price: Price::try_new(100).unwrap(),
                source: ProtectionSource::User
            },
        ]
    );
    let production_maker = pair.production.order(OrderId::new(9)).unwrap();
    let reference_makers = pair.reference.orders(maker_side);
    let reference_maker = reference_makers[0];
    assert_eq!(production_maker.remaining(), 1);
    assert_eq!(reference_maker.total_remaining(), 1);
    assert_eq!(
        production_maker.original_order(),
        reference_maker.original_order()
    );
    assert_eq!(production_maker.priority(), reference_maker.priority());
    assert_eq!(pair.production.next_queue_priority(), 1);
    assert_eq!(pair.production.next_stop_priority(), 2);
    assert_eq!(
        pair.reference.test_reference_state(),
        &pair.production.reference_prices
    );
    assert_eq!(
        pair.production
            .reference_prices
            .latest(PriceReferenceKind::LastTrade),
        Some(matching_domain::PriceReference {
            kind: PriceReferenceKind::LastTrade,
            source_seq: if root_source == PriceReferenceKind::LastTrade {
                3
            } else {
                2
            },
            price: Price::try_new(maker_price).unwrap(),
        })
    );
    if root_source != PriceReferenceKind::LastTrade {
        assert_eq!(
            pair.production.reference_prices.latest(root_source),
            Some(update)
        );
    }
    for id in [OrderId::new(1), OrderId::new(2)] {
        assert!(pair.production.stop_order(id).is_none());
        assert!(pair.reference.stop_order(id).is_none());
        assert!(pair.production.reservation_state(id).is_none());
        assert!(pair.reference.reservation_state(id).is_none());
    }
    assert!(
        pair.production
            .expire_stop_gtd_through(LogicalTime::new(20))
            .is_empty()
    );
    assert!(
        pair.reference
            .expire_stop_gtd_through(LogicalTime::new(20))
            .is_empty()
    );
    pair.validate();
    actual
}

#[test]
fn amend_stop_root_protection_freezes_across_true_descendants() {
    root_frozen_cascade(Side::Buy, false, false, PriceReferenceKind::LastTrade);
    for side in [Side::Buy, Side::Sell] {
        for quote in [false, true] {
            for source in [
                PriceReferenceKind::LastTrade,
                PriceReferenceKind::MarkPrice,
                PriceReferenceKind::IndexPrice,
            ] {
                assert_eq!(
                    root_frozen_cascade(side, quote, false, source),
                    root_frozen_cascade(side, quote, false, source)
                );
            }
        }
    }
}

#[test]
fn amend_stop_root_protection_freezes_for_children_already_in_first_batch() {
    for side in [Side::Buy, Side::Sell] {
        for quote in [false, true] {
            for source in [
                PriceReferenceKind::LastTrade,
                PriceReferenceKind::MarkPrice,
                PriceReferenceKind::IndexPrice,
            ] {
                root_frozen_cascade(side, quote, true, source);
            }
        }
    }
}
