//! 真实改单入口的预留覆盖、验签与提交边界。
use super::*;
use crate::{AmendExecutionContext, ReferenceOrderBook};
use matching_domain::{
    AmendOrder, AssetId, AssetReserve, CommandId, LogicalTime, MarketId, ReservationId,
};
use matching_risk::{ReservationState, RiskKeySet};

pub(super) fn command(input: &LimitGtcOrder) -> AmendOrder {
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
pub(super) fn state(input: &LimitGtcOrder) -> ReservationState {
    ReservationState {
        reservation_id: ReservationId::new(input.order_id.get()),
        order_id: input.order_id,
        user_id: input.user_id,
        market_id: MarketId::new(7),
        version: 11,
        reserves: vec![AssetReserve {
            asset: AssetId::new(3),
            atoms: 1000,
        }],
    }
}

#[test]
fn amend_reservation_no_token_uses_explicit_coverage_and_preserves_version() {
    let keys = RiskKeySet::new(2, []).unwrap();
    let mut p = ProductionOrderBook::new(policy());
    let mut r = ReferenceOrderBook::new();
    let input = order(1, Side::Buy, 100, 10);
    p.rest_with_reservation(input.clone(), state(&input))
        .unwrap();
    r.rest_with_reservation(input.clone(), state(&input))
        .unwrap();
    let mut cmd = command(&input);
    cmd.new_qty = Some(Qty::try_new(8).unwrap());
    let context = AmendExecutionContext {
        market_math: MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        command_id: CommandId::new(80),
        fee_schedule_version: 5,
        fee_tier_id: 1,
        risk_keys: &keys,
        required_reserves: vec![AssetReserve {
            asset: AssetId::new(3),
            atoms: 800,
        }],
    };
    assert_eq!(
        p.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &context)
            .unwrap(),
        r.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &context)
            .unwrap()
    );
    assert_eq!(p.reservation_state(input.order_id), Some(&state(&input)));
    assert_eq!(
        p.reservation_state(input.order_id),
        r.reservation_state(input.order_id)
    );
    let next = p.next_queue_priority();
    let before = r.test_deterministic_state();
    let context = AmendExecutionContext {
        required_reserves: vec![AssetReserve {
            asset: AssetId::new(3),
            atoms: 1001,
        }],
        ..context
    };
    assert_eq!(
        p.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &context),
        Err(RejectReason::InsufficientReservation)
    );
    assert_eq!(
        r.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &context),
        Err(RejectReason::InsufficientReservation)
    );
    assert_eq!(p.next_queue_priority(), next);
    assert_eq!(r.test_deterministic_state(), before);
    assert_eq!(p.order(input.order_id).unwrap().total_remaining(), 8);
}

use super::amend_stop::production_snapshot;
use ed25519_dalek::{Signer, SigningKey};
use matching_domain::{
    IcebergOrder, PriceReferenceKind, QuoteBudget, ReservationAdjustmentToken, StopLimitOrder,
    StopMarketOrder, StopMarketQuantity, StopOrder, TimeInForce, TriggerExpiry,
};

pub(super) fn setup(
    kind: u8,
) -> (
    ProductionOrderBook,
    ReferenceOrderBook,
    LimitGtcOrder,
    AmendOrder,
) {
    setup_side(kind, Side::Buy)
}

pub(super) fn setup_side(
    kind: u8,
    side: Side,
) -> (
    ProductionOrderBook,
    ReferenceOrderBook,
    LimitGtcOrder,
    AmendOrder,
) {
    setup_version(kind, side, 11)
}

pub(super) fn setup_version(
    kind: u8,
    side: Side,
    version: u64,
) -> (
    ProductionOrderBook,
    ReferenceOrderBook,
    LimitGtcOrder,
    AmendOrder,
) {
    let mut p = ProductionOrderBook::new(policy());
    let mut r = ReferenceOrderBook::new();
    let input = order(1, side, 100, 10);
    let mut shadow = state(&input);
    shadow.version = version;
    let mut cmd = command(&input);
    cmd.new_qty = Some(Qty::try_new(12).unwrap());
    match kind {
        0 => {
            p.rest_with_reservation(input.clone(), shadow.clone())
                .unwrap();
            r.rest_with_reservation(input.clone(), shadow.clone())
                .unwrap();
        }
        1 => {
            let ice = IcebergOrder::try_new(input.clone(), Qty::try_new(3).unwrap()).unwrap();
            let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
            p.place_iceberg_post_only_with_reservation(ice.clone(), &math, shadow.clone())
                .unwrap();
            r.place_iceberg_post_only_with_reservation(ice, &math, shadow.clone())
                .unwrap();
        }
        _ => {
            let stop = if kind == 2 {
                StopOrder::Limit(StopLimitOrder {
                    trigger_reference: PriceReferenceKind::LastTrade,
                    order_id: input.order_id,
                    user_id: input.user_id,
                    reservation_id: ReservationId::new(1),
                    fee_schedule_version: 5,
                    side: input.side,
                    stop_price: Price::try_new(110).unwrap(),
                    price: input.price,
                    qty: input.qty,
                    child_tif: TimeInForce::Gtc,
                    trigger_expiry: TriggerExpiry::Gtc,
                })
            } else {
                StopOrder::Market(StopMarketOrder {
                    trigger_reference: PriceReferenceKind::LastTrade,
                    market_protection: None,
                    order_id: input.order_id,
                    user_id: input.user_id,
                    reservation_id: ReservationId::new(1),
                    fee_schedule_version: 5,
                    side: input.side,
                    stop_price: Price::try_new(110).unwrap(),
                    quantity: if kind == 3 {
                        StopMarketQuantity::Quote(QuoteBudget::try_new(1000).unwrap())
                    } else {
                        StopMarketQuantity::Base(input.qty)
                    },
                    child_tif: TimeInForce::Ioc,
                    trigger_expiry: TriggerExpiry::Gtc,
                })
            };
            if kind == 3 {
                cmd.new_qty = None;
                cmd.new_quote_budget = Some(QuoteAmount::new(1200));
            }
            p.place_stop_with_reservation(stop.clone(), LogicalTime::new(1), 9, shadow.clone())
                .unwrap();
            r.place_stop_with_reservation(stop, LogicalTime::new(1), 9, shadow)
                .unwrap();
        }
    }
    (p, r, input, cmd)
}
pub(super) fn token(input: &LimitGtcOrder, signing: &SigningKey) -> ReservationAdjustmentToken {
    let mut token = ReservationAdjustmentToken {
        token_id: 91,
        command_id: CommandId::new(80),
        reservation_id: ReservationId::new(1),
        order_id: input.order_id,
        user_id: input.user_id,
        market_id: MarketId::new(7),
        expected_reservation_version: 11,
        target_reserves: vec![AssetReserve {
            asset: AssetId::new(3),
            atoms: 1250,
        }]
        .into(),
        fee_schedule_version: 5,
        fee_tier_id: 1,
        risk_key_id: 31,
        signature: [0; 64],
    };
    sign(signing, &mut token);
    token
}
pub(super) fn sign(signing: &SigningKey, token: &mut ReservationAdjustmentToken) {
    token.signature = signing
        .sign(&token.canonical_signing_payload().unwrap())
        .to_bytes();
}
pub(super) fn context(keys: &RiskKeySet) -> AmendExecutionContext<'_> {
    AmendExecutionContext {
        market_math: MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
        command_id: CommandId::new(80),
        fee_schedule_version: 5,
        fee_tier_id: 1,
        risk_keys: keys,
        required_reserves: vec![AssetReserve {
            asset: AssetId::new(3),
            atoms: 1200,
        }],
    }
}
fn valid(p: &ProductionOrderBook) {
    assert_eq!(validate_book(p), Ok(()));
    assert_eq!(p.validate_regular_expiry(), Ok(()));
    assert_eq!(p.validate_stop_book(), Ok(()));
}

#[test]
fn amend_reservation_signed_growth_each_container_is_once_and_replay_rejects() {
    let signing = SigningKey::from_bytes(&[7; 32]);
    let keys = RiskKeySet::new(2, [(31, signing.verifying_key())]).unwrap();
    for kind in 0..5 {
        let (mut p, mut r, input, mut cmd) = setup(kind);
        cmd.reservation_adjustment = Some(token(&input, &signing));
        let old_stop = p.next_stop_priority();
        assert_eq!(
            p.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &context(&keys))
                .unwrap(),
            r.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &context(&keys))
                .unwrap()
        );
        assert_eq!(p.reservation_state(input.order_id).unwrap().version, 12);
        assert_eq!(
            p.reservation_state(input.order_id).unwrap().reserves[0].atoms,
            1250
        );
        assert_eq!(
            p.reservation_state(input.order_id),
            r.reservation_state(input.order_id)
        );
        assert_eq!(p.next_stop_priority(), old_stop);
        valid(&p);
        let ps = production_snapshot(&p);
        let rs = r.test_deterministic_state();
        assert_eq!(
            p.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &context(&keys)),
            Err(RejectReason::InvalidReservation)
        );
        assert_eq!(
            r.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &context(&keys)),
            Err(RejectReason::InvalidReservation)
        );
        assert_eq!(production_snapshot(&p), ps);
        assert_eq!(r.test_deterministic_state(), rs);
        let mut next_token = token(&input, &signing);
        next_token.expected_reservation_version = 12;
        next_token.command_id = CommandId::new(81);
        next_token.token_id = 92;
        sign(&signing, &mut next_token);
        cmd.reservation_adjustment = Some(next_token);
        let c = AmendExecutionContext {
            command_id: CommandId::new(81),
            ..context(&keys)
        };
        p.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &c)
            .unwrap();
        r.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &c)
            .unwrap();
        assert_eq!(p.reservation_state(input.order_id).unwrap().version, 13);
        assert_eq!(
            p.reservation_state(input.order_id),
            r.reservation_state(input.order_id)
        );
        valid(&p);
    }
}
#[test]
fn amend_reservation_invalid_tokens_reject_with_full_state_unchanged() {
    let signing = SigningKey::from_bytes(&[7; 32]);
    let keys = RiskKeySet::new(2, [(31, signing.verifying_key())]).unwrap();
    for kind in 0..5 {
        for bad in 0..11 {
            let (mut p, mut r, input, mut cmd) = setup(kind);
            let mut t = token(&input, &signing);
            match bad {
                0 => t.expected_reservation_version = 10,
                1 => t.command_id = CommandId::new(81),
                2 => t.order_id = OrderId::new(2),
                3 => t.user_id = UserId::new(2),
                4 => t.market_id = MarketId::new(8),
                5 => t.reservation_id = ReservationId::new(2),
                6 => t.fee_schedule_version = 6,
                7 => t.fee_tier_id = 2,
                8 => t.risk_key_id = 32,
                9 => t.target_reserves[0].atoms = 1199,
                _ => {}
            }
            sign(&signing, &mut t);
            if bad == 10 {
                t.signature[0] ^= 1;
            }
            cmd.reservation_adjustment = Some(t);
            let ps = production_snapshot(&p);
            let rs = r.test_deterministic_state();
            let expected = Err(if bad == 9 {
                RejectReason::InsufficientReservation
            } else {
                RejectReason::InvalidReservation
            });
            assert_eq!(
                p.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &context(&keys)),
                expected
            );
            assert_eq!(
                r.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &context(&keys)),
                expected
            );
            assert_eq!(production_snapshot(&p), ps);
            assert_eq!(r.test_deterministic_state(), rs);
            valid(&p);
        }
    }
}
#[test]
fn amend_reservation_valid_signature_later_priority_failure_never_applies() {
    let signing = SigningKey::from_bytes(&[7; 32]);
    let keys = RiskKeySet::new(2, [(31, signing.verifying_key())]).unwrap();
    for kind in 0..5 {
        let (mut p, mut r, input, mut cmd) = setup(kind);
        cmd.reservation_adjustment = Some(token(&input, &signing));
        if kind < 2 {
            p.queue_priority_allocator =
                crate::QueuePriorityAllocator::try_restore(policy(), u128::MAX - 1024).unwrap();
            // 参考模型通过自身失败测试入口覆盖真实高水位，不改变生产策略。
            crate::reference_orderbook::amend_candidate::test_exhaust_priority(&mut r, false);
        } else {
            cmd.new_stop_price = Some(Price::try_new(115).unwrap());
            p.stop_priority_allocator =
                crate::StopPriorityAllocator::try_restore(policy(), u128::MAX - 1024).unwrap();
            crate::reference_orderbook::amend_candidate::test_exhaust_priority(&mut r, true);
        }
        let ps = production_snapshot(&p);
        let rs = r.test_deterministic_state();
        assert_eq!(
            p.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &context(&keys)),
            Err(RejectReason::PrioritySpaceExhaustion)
        );
        assert_eq!(
            r.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &context(&keys)),
            Err(RejectReason::PrioritySpaceExhaustion)
        );
        assert_eq!(production_snapshot(&p), ps);
        assert_eq!(r.test_deterministic_state(), rs);
        valid(&p);
    }
}

#[test]
fn amend_reservation_no_token_each_container_checks_coverage_and_keeps_funds() {
    let keys = RiskKeySet::new(2, []).unwrap();
    for kind in 0..5 {
        for reduce in [false, true] {
            let (mut p, mut r, input, mut cmd) = setup(kind);
            if reduce {
                if kind == 3 {
                    cmd.new_quote_budget = Some(QuoteAmount::new(800));
                } else {
                    cmd.new_qty = Some(Qty::try_new(8).unwrap());
                }
            }
            let c = AmendExecutionContext {
                required_reserves: vec![AssetReserve {
                    asset: AssetId::new(3),
                    atoms: if reduce { 800 } else { 1000 },
                }],
                ..context(&keys)
            };
            assert_eq!(
                p.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &c)
                    .unwrap(),
                r.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &c)
                    .unwrap()
            );
            assert_eq!(p.reservation_state(input.order_id), Some(&state(&input)));
            assert_eq!(
                p.reservation_state(input.order_id),
                r.reservation_state(input.order_id)
            );
            valid(&p);
        }
    }
}

#[test]
fn amend_reservation_empty_or_noncanonical_requirement_is_never_implicit_coverage() {
    let keys = RiskKeySet::new(2, []).unwrap();
    let (mut p, mut r, _, cmd) = setup(0);
    for reserves in [
        vec![],
        vec![AssetReserve {
            asset: AssetId::new(3),
            atoms: 0,
        }],
        vec![
            AssetReserve {
                asset: AssetId::new(3),
                atoms: 1,
            },
            AssetReserve {
                asset: AssetId::new(3),
                atoms: 1,
            },
        ],
        vec![
            AssetReserve {
                asset: AssetId::new(3),
                atoms: 1,
            },
            AssetReserve {
                asset: AssetId::new(2),
                atoms: 1,
            },
        ],
    ] {
        let ps = production_snapshot(&p);
        let rs = r.test_deterministic_state();
        let c = AmendExecutionContext {
            required_reserves: reserves,
            ..context(&keys)
        };
        assert_eq!(
            p.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &c),
            Err(RejectReason::InvalidReservation)
        );
        assert_eq!(
            r.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &c),
            Err(RejectReason::InvalidReservation)
        );
        assert_eq!(production_snapshot(&p), ps);
        assert_eq!(r.test_deterministic_state(), rs);
    }
}

#[test]
fn amend_reservation_signed_terminal_regular_and_iceberg_leave_no_shadow() {
    let signing = SigningKey::from_bytes(&[7; 32]);
    let keys = RiskKeySet::new(2, [(31, signing.verifying_key())]).unwrap();
    for kind in 0..2 {
        let (mut p, mut r, input, mut cmd) = setup(kind);
        let maker = order(2, Side::Sell, 110, 10);
        p.rest_with_reservation(maker.clone(), state(&maker))
            .unwrap();
        r.rest_with_reservation(maker.clone(), state(&maker))
            .unwrap();
        cmd.new_qty = None;
        cmd.new_price = Some(Price::try_new(120).unwrap());
        cmd.reservation_adjustment = Some(token(&input, &signing));
        assert_eq!(
            p.amend_with_context(
                &cmd,
                LogicalTime::new(1),
                9,
                &[TradeId::new(77)],
                &context(&keys)
            )
            .unwrap(),
            r.amend_with_context(
                &cmd,
                LogicalTime::new(1),
                9,
                &[TradeId::new(77)],
                &context(&keys)
            )
            .unwrap()
        );
        assert!(p.order(input.order_id).is_none());
        assert!(p.reservation_state(input.order_id).is_none());
        assert!(r.reservation_state(input.order_id).is_none());
        assert!(p.reservation_state(maker.order_id).is_none());
        assert!(r.reservation_state(maker.order_id).is_none());
        valid(&p);
    }
}

#[test]
fn amend_reservation_valid_token_later_stop_expiry_failure_and_version_overflow_are_atomic() {
    let signing = SigningKey::from_bytes(&[7; 32]);
    let keys = RiskKeySet::new(2, [(31, signing.verifying_key())]).unwrap();
    for kind in 2..5 {
        let (mut p, mut r, input, mut cmd) = setup(kind);
        cmd.reservation_adjustment = Some(token(&input, &signing));
        cmd.new_trigger_expiry = Some(matching_domain::TriggerExpiryPatch::Set(
            TriggerExpiry::Gtd {
                expire_at: LogicalTime::new(1),
            },
        ));
        let ps = production_snapshot(&p);
        let rs = r.test_deterministic_state();
        assert_eq!(
            p.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &context(&keys)),
            Err(RejectReason::InvalidExpireTime)
        );
        assert_eq!(
            r.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &context(&keys)),
            Err(RejectReason::InvalidExpireTime)
        );
        assert_eq!(production_snapshot(&p), ps);
        assert_eq!(r.test_deterministic_state(), rs);
        valid(&p);
    }
    let mut p = ProductionOrderBook::new(policy());
    let mut r = ReferenceOrderBook::new();
    let input = order(1, Side::Buy, 100, 10);
    let mut shadow = state(&input);
    shadow.version = u64::MAX;
    p.rest_with_reservation(input.clone(), shadow.clone())
        .unwrap();
    r.rest_with_reservation(input.clone(), shadow).unwrap();
    let mut cmd = command(&input);
    let mut t = token(&input, &signing);
    t.expected_reservation_version = u64::MAX;
    sign(&signing, &mut t);
    cmd.reservation_adjustment = Some(t);
    let ps = production_snapshot(&p);
    let rs = r.test_deterministic_state();
    assert_eq!(
        p.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &context(&keys)),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(
        r.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &context(&keys)),
        Err(RejectReason::ArithmeticOverflow)
    );
    assert_eq!(production_snapshot(&p), ps);
    assert_eq!(r.test_deterministic_state(), rs);
    valid(&p);
}

#[test]
fn amend_reservation_signed_stop_carries_version_into_resting_child_and_next_amend() {
    let signing = SigningKey::from_bytes(&[7; 32]);
    let keys = RiskKeySet::new(2, [(31, signing.verifying_key())]).unwrap();
    let (mut p, mut r, input, mut cmd) = setup(2);
    cmd.reservation_adjustment = Some(token(&input, &signing));
    assert_eq!(
        p.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &context(&keys))
            .unwrap(),
        r.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &context(&keys))
            .unwrap()
    );
    let shadow = p.reservation_state(input.order_id).unwrap().clone();
    let maker = order(2, Side::Sell, 100, 2);
    p.rest(maker.clone()).unwrap();
    r.rest(maker).unwrap();
    let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
    let baseline = matching_domain::PriceReference {
        kind: PriceReferenceKind::LastTrade,
        source_seq: 1,
        price: Price::try_new(90).unwrap(),
    };
    assert_eq!(
        p.collect_triggered_stops(baseline).unwrap(),
        r.collect_triggered_stops(baseline).unwrap()
    );
    let update = matching_domain::PriceReference {
        source_seq: 2,
        price: Price::try_new(110).unwrap(),
        ..baseline
    };
    assert_eq!(
        p.trigger_stops(
            update,
            LogicalTime::new(1),
            9,
            &math,
            &[TradeId::new(1)],
            |_| Ok(())
        )
        .unwrap(),
        r.trigger_stops(
            update,
            LogicalTime::new(1),
            9,
            &math,
            &[TradeId::new(1)],
            |_| Ok(())
        )
        .unwrap()
    );
    assert_eq!(p.order(input.order_id).unwrap().total_remaining(), 10);
    assert_eq!(p.reservation_state(input.order_id), Some(&shadow));
    assert_eq!(r.reservation_state(input.order_id), Some(&shadow));
    valid(&p);
    let mut next = command(&input);
    next.new_qty = Some(Qty::try_new(11).unwrap());
    let c = AmendExecutionContext {
        required_reserves: vec![AssetReserve {
            asset: AssetId::new(3),
            atoms: 900,
        }],
        ..context(&keys)
    };
    assert_eq!(
        p.amend_with_context(&next, LogicalTime::new(1), 9, &[], &c)
            .unwrap(),
        r.amend_with_context(&next, LogicalTime::new(1), 9, &[], &c)
            .unwrap()
    );
    assert_eq!(p.order(input.order_id).unwrap().total_remaining(), 9);
    assert_eq!(p.reservation_state(input.order_id), Some(&shadow));
    valid(&p);
}

#[test]
fn amend_reservation_trigger_reject_market_and_fully_filled_child_clear_shadow() {
    let signing = SigningKey::from_bytes(&[7; 32]);
    let keys = RiskKeySet::new(2, [(31, signing.verifying_key())]).unwrap();
    for kind in 2..5 {
        for reject in [false, true] {
            let (mut p, mut r, input, mut cmd) = setup(kind);
            cmd.reservation_adjustment = Some(token(&input, &signing));
            assert_eq!(
                p.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &context(&keys))
                    .unwrap(),
                r.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &context(&keys))
                    .unwrap()
            );
            let maker = order(2, Side::Sell, 100, 12);
            p.rest(maker.clone()).unwrap();
            r.rest(maker).unwrap();
            let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
            let baseline = matching_domain::PriceReference {
                kind: PriceReferenceKind::LastTrade,
                source_seq: 1,
                price: Price::try_new(90).unwrap(),
            };
            assert_eq!(
                p.collect_triggered_stops(baseline).unwrap(),
                r.collect_triggered_stops(baseline).unwrap()
            );
            let update = matching_domain::PriceReference {
                source_seq: 2,
                price: Price::try_new(110).unwrap(),
                ..baseline
            };
            let pe = p
                .trigger_stops(
                    update,
                    LogicalTime::new(1),
                    9,
                    &math,
                    &[TradeId::new(1)],
                    |_| {
                        if reject {
                            Err(RejectReason::InsufficientReservation)
                        } else {
                            Ok(())
                        }
                    },
                )
                .unwrap();
            let re = r
                .trigger_stops(
                    update,
                    LogicalTime::new(1),
                    9,
                    &math,
                    &[TradeId::new(1)],
                    |_| {
                        if reject {
                            Err(RejectReason::InsufficientReservation)
                        } else {
                            Ok(())
                        }
                    },
                )
                .unwrap();
            assert_eq!(pe, re);
            assert!(p.stop_order(input.order_id).is_none());
            assert!(r.stop_order(input.order_id).is_none());
            assert!(p.order(input.order_id).is_none());
            assert!(p.reservation_state(input.order_id).is_none());
            assert!(r.reservation_state(input.order_id).is_none());
            if reject {
                assert!(!pe.iter().any(|e| matches!(e, EngineEvent::Trade(_))));
                assert!(matches!(
                    pe.as_slice(),
                    [
                        EngineEvent::StopTriggered { .. },
                        EngineEvent::TriggeredOrderRejected { .. },
                        EngineEvent::ReservationReleased { .. }
                    ]
                ));
            } else {
                assert!(
                    pe.iter()
                        .any(|e| matches!(e,EngineEvent::Trade(t) if t.qty.get()==12))
                );
            }
            valid(&p);
        }
    }
}
