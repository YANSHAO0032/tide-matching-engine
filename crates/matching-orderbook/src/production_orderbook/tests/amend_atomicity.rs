//! 每类改单拒绝前后比较完整确定性业务状态，不把双模型一致替代明确拒绝原因。

use super::amend_reservation::{command, setup_version, sign, state, token};
use super::amend_reservation::{context, setup};
use super::amend_stop::production_snapshot;
use super::*;
use crate::ReferenceOrderBook;
use ed25519_dalek::SigningKey;
use matching_domain::LogicalTime;
use matching_domain::{
    AmendOrder, AssetId, AssetReserve, CommandId, MarketId, MarketProtection,
    MarketProtectionPatch, PriceReference, PriceReferenceKind, ReservationId, StopLimitOrder,
    StopOrder, TimeInForce, TriggerExpiry, TriggerExpiryPatch,
};
use matching_risk::RiskKeySet;
use proptest::prelude::*;
use proptest::test_runner::{Config, RngSeed};

struct Pair {
    /// 待差分比较的生产订单簿。
    p: ProductionOrderBook,
    /// 独立参考订单簿。
    r: ReferenceOrderBook,
    /// 本轮生成的改单输入。
    input: LimitOrder,
}

impl Pair {
    fn new(kind: u8, side: Side, noise: u16, version: u64) -> Self {
        let (p, r, input, _) = setup_version(kind, side, version);
        let mut pair = Self { p, r, input };
        if kind < 2 {
            let take = order(
                2,
                if side == Side::Buy {
                    Side::Sell
                } else {
                    Side::Buy
                },
                100,
                3,
            );
            assert_eq!(
                pair.p
                    .place_limit_gtc_with_events(take.clone(), &[TradeId::new(1)])
                    .unwrap(),
                pair.r
                    .place_limit_gtc_with_events(take, &[TradeId::new(1)])
                    .unwrap()
            );
            assert_eq!(
                pair.p.order(pair.input.order_id).unwrap().total_remaining(),
                7
            );
        }
        // 同价伙伴、双侧深度、两类到期索引和多资产预留都必须非空。
        for (id, side, price, tif) in [
            (
                9,
                side,
                100,
                TimeInForce::Gtd {
                    expire_at: LogicalTime::new(40),
                },
            ),
            (7, Side::Buy, 80, TimeInForce::Day { session_id: 9 }),
            (
                4,
                Side::Sell,
                140,
                TimeInForce::Gtd {
                    expire_at: LogicalTime::new(50),
                },
            ),
            (
                6,
                if side == Side::Buy {
                    Side::Sell
                } else {
                    Side::Buy
                },
                if side == Side::Buy { 120 } else { 80 },
                TimeInForce::Day { session_id: 9 },
            ),
        ] {
            let input = order(id, side, price, 2 + u64::from(noise % 5));
            let mut shadow = state(&input);
            shadow.reserves.push(AssetReserve {
                asset: AssetId::new(8),
                atoms: 37 + u128::from(noise),
            });
            assert_eq!(
                pair.p.rest_timed_with_reservation(
                    input.clone(),
                    tif,
                    LogicalTime::new(1),
                    9,
                    shadow.clone()
                ),
                pair.r
                    .rest_timed_with_reservation(input, tif, LogicalTime::new(1), 9, shadow)
            );
        }
        for (id, expiry) in [
            (
                21,
                TriggerExpiry::Gtd {
                    expire_at: LogicalTime::new(35),
                },
            ),
            (18, TriggerExpiry::Day { session_id: 9 }),
        ] {
            let stop = StopOrder::Limit(StopLimitOrder {
                trigger_reference: PriceReferenceKind::LastTrade,
                order_id: OrderId::new(id),
                user_id: UserId::new(id as u64),
                reservation_id: ReservationId::new(id),
                fee_schedule_version: 5,
                side,
                stop_price: Price::try_new(110).unwrap(),
                price: Price::try_new(100).unwrap(),
                qty: Qty::try_new(6).unwrap(),
                child_tif: TimeInForce::Gtc,
                trigger_expiry: expiry,
            });
            let shadow = state(&order(id, side, 100, 6));
            assert_eq!(
                pair.p.place_stop_with_reservation(
                    stop.clone(),
                    LogicalTime::new(1),
                    9,
                    shadow.clone()
                ),
                pair.r
                    .place_stop_with_reservation(stop, LogicalTime::new(1), 9, shadow)
            );
        }
        for kind in [
            PriceReferenceKind::LastTrade,
            PriceReferenceKind::MarkPrice,
            PriceReferenceKind::IndexPrice,
        ] {
            let price = PriceReference {
                kind,
                source_seq: 7,
                price: Price::try_new(90).unwrap(),
            };
            assert_eq!(
                pair.p.collect_triggered_stops(price).unwrap(),
                pair.r.collect_triggered_stops(price).unwrap()
            );
        }
        let removed = order(27, Side::Buy, 70, 2);
        pair.p
            .rest_with_reservation(removed.clone(), state(&removed))
            .unwrap();
        pair.r
            .rest_with_reservation(removed.clone(), state(&removed))
            .unwrap();
        assert!(pair.p.cancel(removed.order_id).is_ok());
        assert!(pair.r.cancel(removed.order_id).is_ok());
        pair.validate();
        pair
    }

    fn validate(&self) {
        assert_eq!(validate_book(&self.p), Ok(()));
        assert_eq!(self.p.validate_regular_expiry(), Ok(()));
        assert_eq!(self.p.validate_stop_book(), Ok(()));
        assert_eq!(
            self.p.next_queue_priority(),
            self.r.test_deterministic_state().1
        );
        assert_eq!(self.p.next_stop_priority(), self.r.next_stop_priority());
        for side in [Side::Buy, Side::Sell] {
            assert_eq!(self.p.stop_orders(side), self.r.stop_orders(side));
            let mut levels = match side {
                Side::Buy => &self.p.bids,
                Side::Sell => &self.p.asks,
            }
            .values()
            .collect::<Vec<_>>();
            if side == Side::Buy {
                levels.reverse();
            }
            let mut po = Vec::new();
            for level in levels {
                let mut cursor = level.head();
                while let Some(index) = cursor {
                    let node = self.p.arena.get(index).unwrap();
                    po.push(node);
                    cursor = node.next();
                }
            }
            let ro = self.r.orders(side);
            assert_eq!(po.len(), ro.len());
            for (p, r) in po.iter().zip(ro) {
                assert_eq!(p.original_order(), r.original_order());
                assert_eq!(
                    (
                        p.visible_remaining(),
                        p.hidden_remaining(),
                        p.priority(),
                        p.time_in_force()
                    ),
                    (
                        r.visible_remaining(),
                        r.hidden_remaining(),
                        r.priority(),
                        r.time_in_force()
                    )
                );
                assert_eq!(
                    self.p.reservation_state(p.original_order().order_id),
                    self.r.reservation_state(p.original_order().order_id)
                );
            }
        }
    }

    fn reject(
        &mut self,
        cmd: &AmendOrder,
        context: &crate::AmendExecutionContext<'_>,
        expected: RejectReason,
    ) {
        self.validate();
        let before_p = production_snapshot(&self.p);
        let before_r = self.r.test_deterministic_state();
        // 同一拒绝连续重放两次，检测首次错误隐藏的版本或分配器漂移。
        for _ in 0..2 {
            assert_eq!(
                self.p
                    .amend_with_context(cmd, LogicalTime::new(10), 9, &[], context),
                Err(expected)
            );
            assert_eq!(
                self.r
                    .amend_with_context(cmd, LogicalTime::new(10), 9, &[], context),
                Err(expected)
            );
            assert_eq!(production_snapshot(&self.p), before_p);
            assert_eq!(self.r.test_deterministic_state(), before_r);
            self.validate();
        }
    }
}

fn reject_case(kind: u8, side: Side, noise: u16, failure: u8) {
    let signing = SigningKey::from_bytes(&[7; 32]);
    let keys = RiskKeySet::new(2, [(31, signing.verifying_key())]).unwrap();
    let mut pair = Pair::new(kind, side, noise, if failure == 32 { u64::MAX } else { 11 });
    let mut cmd = command(&pair.input);
    let mut c = crate::AmendExecutionContext {
        required_reserves: vec![AssetReserve {
            asset: AssetId::new(3),
            atoms: 800,
        }],
        ..context(&keys)
    };
    // 每项都有独立原因断言，不以两个模型同时错误拒绝作为充分证据。
    let expected = match failure {
        0 => {
            cmd.user_id = UserId::new(99);
            RejectReason::InvalidReservation
        }
        1 => {
            cmd.market_id = MarketId::new(99);
            RejectReason::InvalidReservation
        }
        2 => {
            cmd.order_id = OrderId::new(99);
            RejectReason::OrderNotFound
        }
        3 => {
            cmd.new_qty = Some(Qty::try_new(12).unwrap());
            cmd.new_quote_budget = Some(QuoteAmount::new(1200));
            RejectReason::InvalidOrderCombination
        }
        4 => {
            if kind == 3 {
                cmd.new_qty = Some(Qty::try_new(12).unwrap());
            } else {
                cmd.new_quote_budget = Some(QuoteAmount::new(1200));
            }
            RejectReason::InvalidOrderCombination
        }
        5 | 6 if kind < 2 => {
            cmd.new_qty = Some(Qty::try_new(if failure == 5 { 3 } else { 2 }).unwrap());
            RejectReason::InvalidQty
        }
        5 | 6 if kind == 3 => {
            cmd.new_quote_budget = Some(QuoteAmount::new(0));
            RejectReason::InvalidQty
        }
        5..=7 => {
            cmd.new_visible_qty = Some(Qty::try_new(10).unwrap());
            if kind < 2 {
                RejectReason::InvalidIceberg
            } else {
                RejectReason::InvalidOrderCombination
            }
        }
        8 => {
            cmd.new_tif = Some(if kind < 3 {
                TimeInForce::Ioc
            } else {
                TimeInForce::Fok
            });
            RejectReason::InvalidOrderCombination
        }
        9 | 10 if kind < 3 => {
            cmd.new_tif = Some(if failure == 9 {
                TimeInForce::Gtd {
                    expire_at: LogicalTime::new(10),
                }
            } else {
                TimeInForce::Day { session_id: 8 }
            });
            if failure == 9 {
                RejectReason::InvalidExpireTime
            } else {
                RejectReason::InvalidSession
            }
        }
        9 | 10 => {
            cmd.new_price = Some(Price::try_new(101).unwrap());
            RejectReason::InvalidOrderCombination
        }
        11 => {
            cmd.new_market_protection = Some(MarketProtectionPatch::Set(MarketProtection {
                max_slippage_bps: Some(10_000),
                worst_price: None,
            }));
            if kind < 3 {
                RejectReason::InvalidOrderCombination
            } else {
                RejectReason::InvalidSlippageBps
            }
        }
        12 => {
            cmd.new_trigger_reference = Some(PriceReferenceKind::StaticOpening);
            if kind < 2 {
                RejectReason::InvalidOrderCombination
            } else {
                RejectReason::InvalidReferencePriceSource
            }
        }
        13 | 14 => {
            cmd.new_stop_price = Some(Price::try_new(115).unwrap());
            cmd.new_trigger_expiry = Some(TriggerExpiryPatch::Set(if failure == 13 {
                TriggerExpiry::Gtd {
                    expire_at: LogicalTime::new(10),
                }
            } else {
                TriggerExpiry::Day { session_id: 8 }
            }));
            if kind < 2 {
                RejectReason::InvalidOrderCombination
            } else {
                RejectReason::InvalidExpireTime
            }
        }
        15 => {
            if kind < 2 {
                cmd.new_qty = Some(Qty::try_new(12).unwrap());
                pair.p.queue_priority_allocator =
                    crate::QueuePriorityAllocator::try_restore(policy(), u128::MAX - 1024).unwrap();
            } else {
                cmd.new_stop_price = Some(Price::try_new(115).unwrap());
                pair.p.stop_priority_allocator =
                    crate::StopPriorityAllocator::try_restore(policy(), u128::MAX - 1024).unwrap();
            }
            crate::reference_orderbook::test_support::test_exhaust_priority(&mut pair.r, kind >= 2);
            RejectReason::PrioritySpaceExhaustion
        }
        16..=26 | 32 => {
            let mut token = token(&pair.input, &signing);
            match failure {
                17 => token.expected_reservation_version = 10,
                18 => token.command_id = CommandId::new(81),
                19 => token.order_id = OrderId::new(2),
                20 => token.user_id = UserId::new(2),
                21 => token.market_id = MarketId::new(8),
                22 => token.reservation_id = ReservationId::new(2),
                23 => token.fee_schedule_version = 6,
                24 => token.fee_tier_id = 2,
                25 => token.risk_key_id = 32,
                26 => token.target_reserves[0].atoms = 799,
                32 => token.expected_reservation_version = u64::MAX,
                _ => {}
            }
            sign(&signing, &mut token);
            if failure == 16 {
                token.signature[0] ^= 1;
            }
            cmd.reservation_adjustment = Some(token);
            if failure == 26 {
                RejectReason::InsufficientReservation
            } else if failure == 32 {
                RejectReason::ArithmeticOverflow
            } else {
                RejectReason::InvalidReservation
            }
        }
        27 => {
            c.required_reserves[0].atoms = 1001;
            RejectReason::InsufficientReservation
        }
        28 => {
            c.required_reserves.clear();
            RejectReason::InvalidReservation
        }
        29 => {
            c.required_reserves[0].atoms = 0;
            RejectReason::InvalidReservation
        }
        30 => {
            c.required_reserves.push(c.required_reserves[0]);
            RejectReason::InvalidReservation
        }
        31 => {
            c.required_reserves.push(AssetReserve {
                asset: AssetId::new(2),
                atoms: 1,
            });
            RejectReason::InvalidReservation
        }
        _ => unreachable!(),
    };
    pair.reject(&cmd, &c, expected);
}

#[test]
fn amend_atomicity_every_reject_preserves_populated_full_state_in_both_directions() {
    for kind in 0..5 {
        for side in [Side::Buy, Side::Sell] {
            for failure in 0..33 {
                reject_case(kind, side, 17, failure);
            }
        }
    }
}

proptest! {
    #![proptest_config(Config { cases: 64, rng_seed: RngSeed::Fixed(0xA12_FA11), failure_persistence: None, ..Config::default() })]
    #[test]
    fn amend_atomicity_generated_rejections_never_drift(
        kind in 0u8..5, sell in any::<bool>(), noise in any::<u16>(), failures in prop::collection::vec(0u8..33, 1..12),
    ) {
        for failure in failures {
            reject_case(kind, if sell { Side::Sell } else { Side::Buy }, noise, failure);
        }
    }
}

#[test]
fn amend_atomicity_signed_numeric_peak_protection_and_notional_failures_do_not_apply() {
    let signing = SigningKey::from_bytes(&[7; 32]);
    let keys = RiskKeySet::new(2, [(31, signing.verifying_key())]).unwrap();
    for kind in 0..5 {
        for side in [Side::Buy, Side::Sell] {
            let mut pair = Pair::new(kind, side, 4, 11);
            let mut cmd = command(&pair.input);
            cmd.reservation_adjustment = Some(token(&pair.input, &signing));
            let c = context(&keys);
            if kind < 3 {
                let c = crate::AmendExecutionContext {
                    market_math: MarketMath::try_new(0, 0, 38, 1, 1).unwrap(),
                    ..c
                };
                pair.reject(&cmd, &c, RejectReason::ArithmeticOverflow);
            } else {
                cmd.new_market_protection = Some(MarketProtectionPatch::Set(MarketProtection {
                    max_slippage_bps: Some(9),
                    worst_price: Some(Price::try_new(103).unwrap()),
                }));
                let c = crate::AmendExecutionContext {
                    market_math: MarketMath::try_new(0, 0, 0, 5, 1).unwrap(),
                    ..c
                };
                pair.reject(&cmd, &c, RejectReason::InvalidTick);
            }
        }
    }
    let mut pair = Pair::new(1, Side::Buy, 4, 11);
    let mut cmd = command(&pair.input);
    cmd.new_qty = Some(Qty::try_new(9).unwrap());
    cmd.new_visible_qty = Some(Qty::try_new(3).unwrap());
    cmd.reservation_adjustment = Some(token(&pair.input, &signing));
    let c = crate::AmendExecutionContext {
        market_math: MarketMath::try_new(0, 0, 0, 5, 2).unwrap(),
        ..context(&keys)
    };
    pair.reject(&cmd, &c, RejectReason::InvalidLot);
}

#[test]
fn amend_atomicity_actual_maker_notional_overflow_is_precommit_for_regular_and_iceberg() {
    let signing = SigningKey::from_bytes(&[7; 32]);
    let keys = RiskKeySet::new(2, [(31, signing.verifying_key())]).unwrap();
    for kind in 0..2 {
        let mut pair = Pair::new(kind, Side::Sell, 4, 11);
        let mut cmd = command(&pair.input);
        cmd.new_price = Some(Price::try_new(1).unwrap());
        // 先前成交三份，绝对目标五份对应候选余量二份：候选本金 2e38 可表示。
        cmd.new_qty = Some(Qty::try_new(5).unwrap());
        if kind == 1 {
            cmd.new_visible_qty = Some(Qty::try_new(1).unwrap());
        }
        cmd.reservation_adjustment = Some(token(&pair.input, &signing));
        let c = crate::AmendExecutionContext {
            market_math: MarketMath::try_new(0, 0, 38, 1, 1).unwrap(),
            ..context(&keys)
        };
        assert!(
            c.market_math
                .trade_notional(Price::try_new(1).unwrap(), Qty::try_new(2).unwrap())
                .is_ok()
        );
        // 真正 maker 价格八十的成交本金上溢，不能只检查候选限价。
        pair.reject(&cmd, &c, RejectReason::ArithmeticOverflow);
    }
}

#[test]
fn amend_atomicity_trade_id_shortage_is_fail_stop_before_any_state_or_funds_change() {
    let signing = SigningKey::from_bytes(&[7; 32]);
    let keys = RiskKeySet::new(2, [(31, signing.verifying_key())]).unwrap();
    for kind in 0..2 {
        for side in [Side::Buy, Side::Sell] {
            let mut pair = Pair::new(kind, side, 1, 11);
            let mut cmd = command(&pair.input);
            cmd.new_price = Some(Price::try_new(if side == Side::Buy { 120 } else { 80 }).unwrap());
            cmd.new_qty = Some(Qty::try_new(12).unwrap());
            cmd.new_tif = Some(TimeInForce::Gtd {
                expire_at: LogicalTime::new(31),
            });
            cmd.reservation_adjustment = Some(token(&pair.input, &signing));
            let before_p = production_snapshot(&pair.p);
            let before_r = pair.r.test_deterministic_state();
            assert!(
                catch_unwind(AssertUnwindSafe(|| pair.p.amend_with_context(
                    &cmd,
                    LogicalTime::new(10),
                    9,
                    &[],
                    &context(&keys)
                )))
                .is_err()
            );
            assert!(
                catch_unwind(AssertUnwindSafe(|| pair.r.amend_with_context(
                    &cmd,
                    LogicalTime::new(10),
                    9,
                    &[],
                    &context(&keys)
                )))
                .is_err()
            );
            assert_eq!(production_snapshot(&pair.p), before_p);
            assert_eq!(pair.r.test_deterministic_state(), before_r);
            pair.validate();
        }
    }
}

#[test]
fn amend_atomicity_signed_aggregate_overflow_preserves_old_and_target_levels() {
    let signing = SigningKey::from_bytes(&[7; 32]);
    let keys = RiskKeySet::new(2, [(31, signing.verifying_key())]).unwrap();
    for kind in 0..2 {
        for side in [Side::Buy, Side::Sell] {
            for reprice in [false, true] {
                let mut pair = Pair::new(kind, side, 1, 11);
                let price = if reprice {
                    if side == Side::Buy { 95 } else { 105 }
                } else {
                    100
                };
                let huge = order(
                    31,
                    side,
                    price,
                    if reprice { u64::MAX } else { u64::MAX - 10 },
                );
                assert_eq!(
                    pair.p.rest_with_reservation(huge.clone(), state(&huge)),
                    pair.r.rest_with_reservation(huge.clone(), state(&huge))
                );
                let mut cmd = command(&pair.input);
                cmd.new_qty = Some(Qty::try_new(30).unwrap());
                if kind == 1 {
                    cmd.new_visible_qty = Some(Qty::try_new(20).unwrap());
                }
                if reprice {
                    cmd.new_price = Some(Price::try_new(price).unwrap());
                }
                cmd.new_tif = Some(TimeInForce::Gtd {
                    expire_at: LogicalTime::new(31),
                });
                cmd.reservation_adjustment = Some(token(&pair.input, &signing));
                pair.reject(&cmd, &context(&keys), RejectReason::ArithmeticOverflow);
            }
        }
    }
}

#[test]
fn amend_atomicity_maker_replenishment_priority_failure_preserves_every_container() {
    let signing = SigningKey::from_bytes(&[7; 32]);
    let keys = RiskKeySet::new(2, [(31, signing.verifying_key())]).unwrap();
    for kind in 0..2 {
        for side in [Side::Buy, Side::Sell] {
            let mut pair = Pair::new(kind, side, 1, 11);
            let maker = order(
                31,
                if side == Side::Buy {
                    Side::Sell
                } else {
                    Side::Buy
                },
                if side == Side::Buy { 115 } else { 85 },
                6,
            );
            let ice =
                matching_domain::IcebergOrder::try_new(maker.clone(), Qty::try_new(2).unwrap())
                    .unwrap();
            let math = MarketMath::try_new(0, 0, 0, 1, 1).unwrap();
            assert_eq!(
                pair.p
                    .place_iceberg_post_only_with_reservation(ice.clone(), &math, state(&maker)),
                pair.r
                    .place_iceberg_post_only_with_reservation(ice, &math, state(&maker))
            );
            let mut cmd = command(&pair.input);
            cmd.new_price = Some(maker.price);
            cmd.new_tif = Some(TimeInForce::Day { session_id: 9 });
            cmd.reservation_adjustment = Some(token(&pair.input, &signing));
            pair.p.queue_priority_allocator =
                crate::QueuePriorityAllocator::try_restore(policy(), u128::MAX - 1024).unwrap();
            crate::reference_orderbook::test_support::test_exhaust_priority(&mut pair.r, false);
            pair.reject(&cmd, &context(&keys), RejectReason::PrioritySpaceExhaustion);
        }
    }
}

#[test]
fn amend_atomicity_numeric_alignment_rejects_before_reservation_or_book_changes() {
    let keys = RiskKeySet::new(2, []).unwrap();
    for kind in 0..5 {
        for bad_price in [true, false] {
            let (mut p, mut r, _, mut cmd) = setup(kind);
            cmd.new_qty = None;
            cmd.new_quote_budget = None;
            if kind == 1 {
                cmd.new_visible_qty = Some(Qty::try_new(4).unwrap());
            }
            if bad_price {
                if kind < 3 {
                    cmd.new_price = Some(Price::try_new(103).unwrap());
                } else {
                    cmd.new_stop_price = Some(Price::try_new(113).unwrap());
                }
            } else if kind == 3 {
                // 报价模式没有基础数量步长；单独检验触发价对齐。
                cmd.new_stop_price = Some(Price::try_new(113).unwrap());
            } else {
                cmd.new_qty = Some(Qty::try_new(11).unwrap());
            }
            let c = crate::AmendExecutionContext {
                market_math: MarketMath::try_new(0, 0, 0, 5, 2).unwrap(),
                required_reserves: vec![matching_domain::AssetReserve {
                    asset: matching_domain::AssetId::new(3),
                    atoms: 800,
                }],
                ..context(&keys)
            };
            let before_p = production_snapshot(&p);
            let before_r = r.test_deterministic_state();
            let expected = Err(if bad_price || kind == 3 {
                RejectReason::InvalidTick
            } else {
                RejectReason::InvalidLot
            });
            assert_eq!(
                p.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &c),
                expected
            );
            assert_eq!(
                r.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &c),
                expected
            );
            assert_eq!(production_snapshot(&p), before_p);
            assert_eq!(r.test_deterministic_state(), before_r);
            assert_eq!(validate_book(&p), Ok(()));
            assert_eq!(p.validate_regular_expiry(), Ok(()));
            assert_eq!(p.validate_stop_book(), Ok(()));
        }
    }
}

#[test]
fn amend_atomicity_numeric_nonunit_market_has_valid_positive_controls() {
    let keys = RiskKeySet::new(2, []).unwrap();
    for kind in 0..5 {
        let (mut p, mut r, input, mut cmd) = setup(kind);
        if kind == 1 {
            cmd.new_visible_qty = Some(Qty::try_new(4).unwrap());
        }
        let c = crate::AmendExecutionContext {
            market_math: MarketMath::try_new(0, 0, 0, 5, 2).unwrap(),
            required_reserves: vec![AssetReserve {
                asset: AssetId::new(3),
                atoms: 800,
            }],
            ..context(&keys)
        };
        let events = p
            .amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &c)
            .unwrap();
        assert_eq!(
            r.amend_with_context(&cmd, LogicalTime::new(1), 9, &[], &c)
                .unwrap(),
            events
        );
        assert_eq!(p.reservation_state(input.order_id), Some(&state(&input)));
        assert_eq!(validate_book(&p), Ok(()));
        assert_eq!(p.validate_regular_expiry(), Ok(()));
        assert_eq!(p.validate_stop_book(), Ok(()));
    }
}
