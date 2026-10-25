//! 冰山改单两模型的守恒、优先级与终结证据。
use super::*;
use crate::ReferenceOrderBook;
use matching_domain::{AmendOrder, IcebergOrder, LogicalTime, MarketId, ReservationId};
use matching_risk::ReservationState;

fn command(input: &LimitGtcOrder) -> AmendOrder {
    AmendOrder {
        market_id: MarketId::new(7),
        user_id: input.user_id,
        order_id: input.order_id,
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
fn shadow(input: &LimitGtcOrder) -> ReservationState {
    ReservationState {
        reservation_id: ReservationId::new(input.order_id.get()),
        order_id: input.order_id,
        user_id: input.user_id,
        market_id: MarketId::new(7),
        version: 11,
        reserves: vec![],
    }
}

#[test]
fn amend_iceberg_matrix_preserves_only_nonincreasing_exposure() {
    for side in [Side::Buy, Side::Sell] {
        for (total, peak, price, retained, visible, hidden) in [
            (8, 3, 100, true, 3, 5),
            (8, 2, 100, true, 2, 6),
            (12, 3, 100, false, 3, 9),
            (8, 4, 100, false, 4, 4),
            (10, 3, 99, false, 3, 7),
        ] {
            let mut p = ProductionOrderBook::new(policy());
            let mut r = ReferenceOrderBook::new();
            let input = order(1, side, 100, 10);
            let ice = IcebergOrder::try_new(input.clone(), Qty::try_new(3).unwrap()).unwrap();
            let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
            p.place_iceberg_post_only_with_reservation(ice.clone(), &math, shadow(&input))
                .unwrap();
            r.place_iceberg_post_only_with_reservation(ice, &math, shadow(&input))
                .unwrap();
            let peer = order(2, side, 100, 2);
            p.rest(peer.clone()).unwrap();
            r.rest(peer).unwrap();
            let old = p.order(input.order_id).unwrap().priority();
            let mut cmd = command(&input);
            cmd.new_qty = Some(Qty::try_new(total).unwrap());
            cmd.new_visible_qty = Some(Qty::try_new(peak).unwrap());
            cmd.new_price = Some(Price::try_new(price).unwrap());
            let pe = p
                .amend_regular_with_events(&cmd, LogicalTime::new(1), 9, &[])
                .unwrap();
            let re = r
                .amend_regular_with_events(&cmd, LogicalTime::new(1), 9, &[])
                .unwrap();
            assert_eq!(pe, re);
            assert!(
                matches!(pe[0], EngineEvent::OrderAmended { priority_retained, .. } if priority_retained == retained)
            );
            let pn = p.order(input.order_id).unwrap();
            let entries = r.orders(side);
            let rn = entries
                .iter()
                .find(|e| e.original_order().order_id == input.order_id)
                .unwrap();
            assert_eq!(
                (
                    pn.visible_remaining(),
                    pn.hidden_remaining(),
                    pn.total_remaining()
                ),
                (visible, hidden, total)
            );
            assert_eq!(
                (
                    rn.visible_remaining(),
                    rn.hidden_remaining(),
                    rn.total_remaining()
                ),
                (visible, hidden, total)
            );
            assert_eq!(pn.priority() == old, retained);
            assert_eq!(pn.priority(), rn.priority());
            if price == 100 {
                assert_eq!(
                    r.best(side).unwrap().original_order().order_id,
                    OrderId::new(if retained { 1 } else { 2 })
                );
            }
            let level = match side {
                Side::Buy => &p.bids,
                Side::Sell => &p.asks,
            };
            let target = level.get(&Price::try_new(price).unwrap()).unwrap();
            if price == 100 {
                assert_eq!(
                    p.arena
                        .get(target.head().unwrap())
                        .unwrap()
                        .original_order()
                        .order_id,
                    OrderId::new(if retained { 1 } else { 2 })
                );
            }
            assert_eq!(validate_book(&p), Ok(()));
            assert_eq!(p.validate_regular_expiry(), Ok(()));
            assert_eq!(p.validate_stop_book(), Ok(()));
        }
    }
}

use super::amend_stop::production_snapshot;
use matching_domain::{AmendedQuantity, TimeInForce};

fn pair(
    side: Side,
    price: i64,
    total: u64,
    peak: u64,
) -> (ProductionOrderBook, ReferenceOrderBook, LimitGtcOrder) {
    let mut p = ProductionOrderBook::new(policy());
    let mut r = ReferenceOrderBook::new();
    let input = order(1, side, price, total);
    let ice = IcebergOrder::try_new(input.clone(), Qty::try_new(peak).unwrap()).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    p.place_iceberg_post_only_with_reservation(ice.clone(), &math, shadow(&input))
        .unwrap();
    r.place_iceberg_post_only_with_reservation(ice, &math, shadow(&input))
        .unwrap();
    (p, r, input)
}
fn valid(p: &ProductionOrderBook) {
    assert_eq!(validate_book(p), Ok(()));
    assert_eq!(p.validate_regular_expiry(), Ok(()));
    assert_eq!(p.validate_stop_book(), Ok(()));
}

#[test]
fn amend_iceberg_partial_fill_absolute_target_does_not_reveal_hidden_and_expiry_migrates() {
    for side in [Side::Buy, Side::Sell] {
        let (mut p, mut r, input) = pair(side, 100, 10, 4);
        let opposite = if side == Side::Buy {
            Side::Sell
        } else {
            Side::Buy
        };
        let take = order(2, opposite, 100, 2);
        assert_eq!(
            p.place_limit_gtc_with_events(take.clone(), &[TradeId::new(1)])
                .unwrap(),
            r.place_limit_gtc_with_events(take, &[TradeId::new(1)])
                .unwrap()
        );
        let old = p.order(input.order_id).unwrap().priority();
        let mut cmd = command(&input);
        cmd.new_qty = Some(Qty::try_new(7).unwrap());
        cmd.new_tif = Some(TimeInForce::Gtd {
            expire_at: LogicalTime::new(20),
        });
        let pe = p
            .amend_regular_with_events(&cmd, LogicalTime::new(1), 9, &[])
            .unwrap();
        assert_eq!(
            pe,
            r.amend_regular_with_events(&cmd, LogicalTime::new(1), 9, &[])
                .unwrap()
        );
        assert!(
            matches!(pe[0],EngineEvent::OrderAmended {old_quantity:AmendedQuantity::BaseRemaining(old),new_quantity:AmendedQuantity::BaseRemaining(new),priority_retained:true,..} if old.get()==8 && new.get()==5)
        );
        let node = p.order(input.order_id).unwrap();
        assert_eq!(
            (
                node.original_order().qty.get(),
                node.visible_remaining(),
                node.hidden_remaining(),
                node.total_remaining()
            ),
            (7, 2, 3, 5)
        );
        assert_eq!(node.priority(), old);
        valid(&p);
        cmd.new_qty = Some(Qty::try_new(6).unwrap());
        cmd.new_visible_qty = Some(Qty::try_new(2).unwrap());
        cmd.new_tif = Some(TimeInForce::Gtd {
            expire_at: LogicalTime::new(30),
        });
        assert_eq!(
            p.amend_regular_with_events(&cmd, LogicalTime::new(1), 9, &[])
                .unwrap(),
            r.amend_regular_with_events(&cmd, LogicalTime::new(1), 9, &[])
                .unwrap()
        );
        assert_eq!(
            p.expire_gtd_through(LogicalTime::new(20)),
            r.expire_gtd_through(LogicalTime::new(20))
        );
        assert!(p.order(input.order_id).is_some());
        valid(&p);
        assert_eq!(
            p.expire_gtd_through(LogicalTime::new(30)),
            r.expire_gtd_through(LogicalTime::new(30))
        );
        assert!(p.order(input.order_id).is_none());
        assert!(p.reservation_state(input.order_id).is_none());
        assert!(r.reservation_state(input.order_id).is_none());
        valid(&p);
    }
}

#[test]
fn amend_iceberg_reprice_original_order_events_and_residual_metadata() {
    for side in [Side::Buy, Side::Sell] {
        for maker_qty in [4, 10] {
            let (old_price, new_price) = if side == Side::Buy {
                (100, 120)
            } else {
                (120, 100)
            };
            let (mut p, mut r, input) = pair(side, old_price, 10, 3);
            let maker = order(
                2,
                if side == Side::Buy {
                    Side::Sell
                } else {
                    Side::Buy
                },
                110,
                maker_qty,
            );
            p.rest_with_reservation(maker.clone(), shadow(&maker))
                .unwrap();
            r.rest_with_reservation(maker.clone(), shadow(&maker))
                .unwrap();
            let next = p.next_queue_priority();
            let mut cmd = command(&input);
            cmd.new_price = Some(Price::try_new(new_price).unwrap());
            cmd.new_tif = Some(TimeInForce::Day { session_id: 9 });
            let pe = p
                .amend_regular_with_events(&cmd, LogicalTime::new(1), 9, &[TradeId::new(77)])
                .unwrap();
            let re = r
                .amend_regular_with_events(&cmd, LogicalTime::new(1), 9, &[TradeId::new(77)])
                .unwrap();
            assert_eq!(pe, re);
            assert_eq!(
                pe[0],
                EngineEvent::OrderAmended {
                    order_id: input.order_id,
                    old_price: Some(input.price),
                    new_price: cmd.new_price,
                    old_quantity: AmendedQuantity::BaseRemaining(input.qty),
                    new_quantity: AmendedQuantity::BaseRemaining(input.qty),
                    priority_retained: false
                }
            );
            assert!(
                matches!(&pe[1],EngineEvent::Trade(t) if t.trade_id==TradeId::new(77) && t.price.get()==110 && t.qty.get()==maker_qty)
            );
            assert_eq!(pe.len(), 3);
            assert!(p.reservation_state(maker.order_id).is_none());
            assert!(r.reservation_state(maker.order_id).is_none());
            if maker_qty == 10 {
                assert_eq!(
                    pe[2],
                    EngineEvent::OrderFilled {
                        order_id: input.order_id
                    }
                );
                assert!(p.order(input.order_id).is_none());
                assert!(p.reservation_state(input.order_id).is_none());
                assert!(r.reservation_state(input.order_id).is_none());
                assert_eq!(p.next_queue_priority(), next);
            } else {
                assert_eq!(
                    pe[2],
                    EngineEvent::OrderPartiallyFilled {
                        order_id: input.order_id,
                        remaining: Qty::try_new(6).unwrap()
                    }
                );
                let node = p.order(input.order_id).unwrap();
                assert_eq!(
                    (
                        node.total_remaining(),
                        node.visible_remaining(),
                        node.hidden_remaining()
                    ),
                    (6, 3, 3)
                );
                assert_eq!(node.peak_size(), Some(Qty::try_new(3).unwrap()));
                assert_eq!(node.time_in_force(), TimeInForce::Day { session_id: 9 });
                assert_eq!(node.priority().get(), next);
                assert_eq!(p.end_session(9, 9).unwrap(), r.end_session(9, 9).unwrap());
                assert!(p.reservation_state(input.order_id).is_none());
            }
            valid(&p);
        }
    }
}

#[test]
fn amend_iceberg_invalid_candidate_and_short_trade_ids_preserve_entire_state() {
    for side in [Side::Buy, Side::Sell] {
        let (mut p, mut r, input) = pair(side, 100, 10, 3);
        for bad in 0..4 {
            let mut cmd = command(&input);
            let expected = match bad {
                0 => {
                    cmd.new_visible_qty = Some(Qty::try_new(10).unwrap());
                    RejectReason::InvalidIceberg
                }
                1 => {
                    cmd.new_tif = Some(TimeInForce::Gtd {
                        expire_at: LogicalTime::new(1),
                    });
                    RejectReason::InvalidExpireTime
                }
                2 => {
                    cmd.new_tif = Some(TimeInForce::Day { session_id: 8 });
                    RejectReason::InvalidSession
                }
                _ => {
                    cmd.new_tif = Some(TimeInForce::Ioc);
                    RejectReason::InvalidOrderCombination
                }
            };
            let ps = production_snapshot(&p);
            let rs = r.test_deterministic_state();
            assert_eq!(
                p.amend_regular_with_events(&cmd, LogicalTime::new(1), 9, &[]),
                Err(expected)
            );
            assert_eq!(
                r.amend_regular_with_events(&cmd, LogicalTime::new(1), 9, &[]),
                Err(expected)
            );
            assert_eq!(production_snapshot(&p), ps);
            assert_eq!(r.test_deterministic_state(), rs);
            valid(&p);
        }
        let maker = order(
            2,
            if side == Side::Buy {
                Side::Sell
            } else {
                Side::Buy
            },
            if side == Side::Buy { 110 } else { 90 },
            2,
        );
        p.rest(maker.clone()).unwrap();
        r.rest(maker).unwrap();
        let mut cmd = command(&input);
        cmd.new_price = Some(Price::try_new(if side == Side::Buy { 120 } else { 80 }).unwrap());
        let ps = production_snapshot(&p);
        let rs = r.test_deterministic_state();
        assert!(
            catch_unwind(AssertUnwindSafe(|| p.amend_regular_with_events(
                &cmd,
                LogicalTime::new(1),
                9,
                &[]
            )))
            .is_err()
        );
        assert!(
            catch_unwind(AssertUnwindSafe(|| r.amend_regular_with_events(
                &cmd,
                LogicalTime::new(1),
                9,
                &[]
            )))
            .is_err()
        );
        assert_eq!(production_snapshot(&p), ps);
        assert_eq!(r.test_deterministic_state(), rs);
        valid(&p);
    }
}

#[test]
fn amend_iceberg_tail_peak_is_valid_against_absolute_total_and_zero_target_rejects() {
    for side in [Side::Buy, Side::Sell] {
        let (mut p, mut r, input) = pair(side, 100, 10, 4);
        let take = order(
            2,
            if side == Side::Buy {
                Side::Sell
            } else {
                Side::Buy
            },
            100,
            7,
        );
        let ids = [TradeId::new(1), TradeId::new(2)];
        assert_eq!(
            p.place_limit_gtc_with_events(take.clone(), &ids).unwrap(),
            r.place_limit_gtc_with_events(take, &ids).unwrap()
        );
        let mut cmd = command(&input);
        cmd.new_qty = Some(Qty::try_new(9).unwrap());
        assert_eq!(
            p.amend_regular_with_events(&cmd, LogicalTime::new(1), 9, &[])
                .unwrap(),
            r.amend_regular_with_events(&cmd, LogicalTime::new(1), 9, &[])
                .unwrap()
        );
        let node = p.order(input.order_id).unwrap();
        assert_eq!(
            (
                node.original_order().qty.get(),
                node.visible_remaining(),
                node.hidden_remaining(),
                node.total_remaining(),
                node.peak_size().unwrap().get()
            ),
            (9, 1, 1, 2, 4)
        );
        for target in [6, 7] {
            cmd.new_qty = Some(Qty::try_new(target).unwrap());
            let ps = production_snapshot(&p);
            let rs = r.test_deterministic_state();
            assert_eq!(
                p.amend_regular_with_events(&cmd, LogicalTime::new(1), 9, &[]),
                Err(RejectReason::InvalidQty)
            );
            assert_eq!(
                r.amend_regular_with_events(&cmd, LogicalTime::new(1), 9, &[]),
                Err(RejectReason::InvalidQty)
            );
            assert_eq!(production_snapshot(&p), ps);
            assert_eq!(r.test_deterministic_state(), rs);
            valid(&p);
        }
    }
}

#[test]
fn amend_iceberg_visible_aggregate_overflow_precedes_writes_for_requeue_and_reprice() {
    for side in [Side::Buy, Side::Sell] {
        for reprice in [false, true] {
            let (mut p, mut r, input) = pair(side, 100, 10, 3);
            let peer = order(
                2,
                side,
                if reprice { 90 } else { 100 },
                u64::MAX - if reprice { 2 } else { 3 },
            );
            p.rest(peer.clone()).unwrap();
            r.rest(peer).unwrap();
            let mut cmd = command(&input);
            if reprice {
                cmd.new_price = Some(Price::try_new(90).unwrap());
            } else {
                cmd.new_visible_qty = Some(Qty::try_new(4).unwrap());
            }
            let ps = production_snapshot(&p);
            let rs = r.test_deterministic_state();
            assert_eq!(
                p.amend_regular_with_events(&cmd, LogicalTime::new(1), 9, &[]),
                Err(RejectReason::ArithmeticOverflow)
            );
            assert_eq!(
                r.amend_regular_with_events(&cmd, LogicalTime::new(1), 9, &[]),
                Err(RejectReason::ArithmeticOverflow)
            );
            assert_eq!(production_snapshot(&p), ps);
            assert_eq!(r.test_deterministic_state(), rs);
            valid(&p);
        }
    }
}
