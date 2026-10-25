//! 固定边界先于长命令序列验收；每一步都通过完整上下文和状态比较。
use super::*;

fn ok(h: &mut Harness, command: Command) -> Record {
    let record = h.run(&command);
    assert!(
        !record.outcome.failed(),
        "{command:?}: {:?}",
        record.outcome
    );
    record
}

fn reject(h: &mut Harness, command: Command, reason: RejectReason) {
    assert_eq!(h.run(&command).outcome, Outcome::Events(Err(reason)));
}

pub(super) fn initialize(h: &mut Harness, id: u128, kind: u8, side: Side) {
    let input = order(id, side, if side == Side::Buy { 90 } else { 110 }, 12);
    let command = match kind {
        0 => Command::Rest(input, gtd(100)),
        1 => Command::Iceberg(IcebergOrder::try_new(input, Qty::try_new(3).unwrap()).unwrap()),
        _ => Command::Stop(stop(
            id,
            kind,
            side,
            TriggerExpiry::Gtd {
                expire_at: LogicalTime::new(100),
            },
        )),
    };
    ok(h, command);
}

#[test]
fn amend_full_fixed_regular_mutations_preserve_unrelated_stop_reservations() {
    for branch in 0..5 {
        let mut h = Harness::new();
        initialize(&mut h, 5, 2, Side::Buy);
        initialize(&mut h, 7, 3, Side::Sell);
        initialize(&mut h, 9, 4, Side::Buy);
        initialize(&mut h, 1, if branch == 2 { 1 } else { 0 }, Side::Buy);
        match branch {
            0 => {
                ok(
                    &mut h,
                    Command::Place(order(2, Side::Sell, 90, 3), TimeInForce::Gtc),
                );
            }
            1 | 2 => {
                let mut p = patch(1);
                p.new_price = Some(Price::try_new(91).unwrap());
                ok(&mut h, amend(p));
            }
            _ => {
                let take = order(2, Side::Sell, 90, 3);
                let trades = [TradeId::new(1)];
                let math = math();
                let r = if branch == 3 {
                    h.r.place_limit_ioc(take.clone(), &math, &trades)
                } else {
                    h.r.place_limit_fok(take.clone(), &math, &trades)
                };
                let p = if branch == 3 {
                    h.p.place_limit_ioc(take, &math, &trades)
                } else {
                    h.p.place_limit_fok(take, &math, &trades)
                };
                assert_eq!(r, p);
                assert_eq!(
                    reference_state(&h.r, h.clock.now()),
                    production_state(&h.p, h.clock.now())
                );
            }
        }
        for id in [5, 7, 9] {
            assert_eq!(
                h.r.reservation_state(OrderId::new(id)),
                h.p.reservation_state(OrderId::new(id))
            );
            assert!(h.p.reservation_state(OrderId::new(id)).is_some());
        }
    }
}

#[test]
fn amend_full_fixed_valid_token_then_aggregate_preflight_failure_never_applies() {
    for side in [Side::Buy, Side::Sell] {
        for kind in 0..2 {
            let mut h = Harness::new();
            initialize(&mut h, 1, kind, side);
            let price = if side == Side::Buy { 90 } else { 110 };
            let visible = if kind == 0 { 12 } else { 3 };
            let partner = order(2, side, price, u64::MAX - visible);
            h.r.rest(partner.clone()).unwrap();
            h.p.rest(partner).unwrap();
            let before_p = super::super::super::amend_stop::production_snapshot(&h.p);
            let before_r = h.r.test_deterministic_state();
            let mut p = patch(1);
            if kind == 0 {
                p.new_qty = Some(Qty::try_new(13).unwrap());
            } else {
                p.new_visible_qty = Some(Qty::try_new(4).unwrap());
            }
            signed(&mut p, 80, 11);
            let context = AmendExecutionContext {
                market_math: math(),
                command_id: CommandId::new(80),
                fee_schedule_version: 5,
                fee_tier_id: 1,
                risk_keys: &h.keys,
                required_reserves: reserves(11_000),
            };
            assert_eq!(
                h.p.amend_with_context(&p, h.clock.now(), SESSION, &[], &context),
                Err(RejectReason::ArithmeticOverflow)
            );
            assert_eq!(
                h.r.amend_with_context(&p, h.clock.now(), SESSION, &[], &context),
                Err(RejectReason::ArithmeticOverflow)
            );
            assert_eq!(
                super::super::super::amend_stop::production_snapshot(&h.p),
                before_p
            );
            assert_eq!(h.r.test_deterministic_state(), before_r);
            assert_eq!(
                reference_state(&h.r, h.clock.now()),
                production_state(&h.p, h.clock.now())
            );
            assert_eq!(h.p.reservation_state(OrderId::new(1)).unwrap().version, 11);
            // 同一有效令牌在安全减量后能真正应用，排除签名错误导致的假拒绝。
            p.new_qty = Some(Qty::try_new(11).unwrap());
            p.new_visible_qty = if kind == 1 {
                Some(Qty::try_new(2).unwrap())
            } else {
                None
            };
            assert_eq!(
                h.r.amend_with_context(&p, h.clock.now(), SESSION, &[], &context)
                    .unwrap(),
                h.p.amend_with_context(&p, h.clock.now(), SESSION, &[], &context)
                    .unwrap()
            );
            assert_eq!(h.p.reservation_state(OrderId::new(1)).unwrap().version, 12);
            assert_eq!(
                reference_state(&h.r, h.clock.now()),
                production_state(&h.p, h.clock.now())
            );
        }
    }
}

#[test]
fn amend_full_fixed_regular_absolute_targets_requeue_and_tif_matrix() {
    for side in [Side::Buy, Side::Sell] {
        let mut h = Harness::new();
        initialize(&mut h, 1, 0, side);
        ok(
            &mut h,
            Command::Rest(
                order(2, side, if side == Side::Buy { 90 } else { 110 }, 4),
                TimeInForce::Gtc,
            ),
        );
        ok(
            &mut h,
            Command::Place(
                order(
                    3,
                    opposite_side(side),
                    if side == Side::Buy { 90 } else { 110 },
                    3,
                ),
                TimeInForce::Gtc,
            ),
        );
        let mut p = patch(1);
        p.new_qty = Some(Qty::try_new(9).unwrap());
        let reduced = ok(&mut h, amend(p));
        assert_eq!(reduced.state.regular(OrderId::new(1)).unwrap().total, 6);
        for target in [2, 3] {
            let mut p = patch(1);
            p.new_qty = Some(Qty::try_new(target).unwrap());
            reject(&mut h, amend(p), RejectReason::InvalidQty);
        }
        let mut p = patch(1);
        p.new_qty = Some(Qty::try_new(15).unwrap());
        let increased = ok(&mut h, amend(p));
        let ids = increased
            .state
            .regular
            .iter()
            .map(|o| o.original.order_id.get())
            .collect::<Vec<_>>();
        assert_eq!(ids, [2, 1]);
        let mut p = patch(1);
        p.new_price = Some(Price::try_new(if side == Side::Buy { 89 } else { 111 }).unwrap());
        ok(&mut h, amend(p));
        for tif in [
            TimeInForce::Gtc,
            TimeInForce::Day {
                session_id: SESSION,
            },
            gtd(30),
            gtd(20),
        ] {
            let mut p = patch(1);
            p.new_tif = Some(tif);
            ok(&mut h, amend(p));
        }
        for (tif, reason) in [
            (gtd(1), RejectReason::InvalidExpireTime),
            (
                TimeInForce::Day { session_id: 8 },
                RejectReason::InvalidSession,
            ),
        ] {
            let mut p = patch(1);
            p.new_tif = Some(tif);
            reject(&mut h, amend(p), reason);
        }
        let expired = ok(&mut h, Command::AdvanceTime(LogicalTime::new(20)));
        assert!(expired.state.regular(OrderId::new(1)).is_none());
        assert!(expired.state.reservation(OrderId::new(1)).is_none());
    }
}

#[test]
fn amend_full_fixed_reprice_multilevel_events_and_only_residual_allocates() {
    for side in [Side::Buy, Side::Sell] {
        for remaining in [5, 8] {
            let mut h = Harness::new();
            let old = if side == Side::Buy { 90 } else { 110 };
            let target = if side == Side::Buy { 101 } else { 99 };
            ok(
                &mut h,
                Command::Rest(order(1, side, old, remaining), gtd(20)),
            );
            for (id, price, qty) in [(2, 100, 2), (3, target, 3)] {
                ok(
                    &mut h,
                    Command::Rest(order(id, opposite_side(side), price, qty), gtd(30)),
                );
            }
            let next = h.p.next_queue_priority();
            let mut p = patch(1);
            p.new_price = Some(Price::try_new(target).unwrap());
            p.new_tif = Some(gtd(40));
            let result = ok(&mut h, amend(p));
            assert!(matches!(
                result.outcome.events(),
                [
                    EngineEvent::OrderAmended { .. },
                    EngineEvent::Trade(_),
                    EngineEvent::Trade(_),
                    _
                ]
            ));
            assert_eq!(result.state.next, next + u128::from(remaining > 5));
            assert_eq!(
                result.state.regular(OrderId::new(1)).map(|o| o.total),
                if remaining == 5 { None } else { Some(3) }
            );
        }
    }
}

#[test]
fn amend_full_fixed_stop_fields_expiry_priority_and_quantity_modes() {
    for side in [Side::Buy, Side::Sell] {
        for kind in 2..5 {
            let mut h = Harness::new();
            initialize(&mut h, 1, kind, side);
            initialize(&mut h, 2, kind, side);
            for qty in [8, 15] {
                let mut p = patch(1);
                if kind == 3 {
                    p.new_quote_budget = Some(QuoteAmount::new(u128::from(qty) * 100));
                } else {
                    p.new_qty = Some(Qty::try_new(qty).unwrap());
                }
                if kind == 2 {
                    p.new_price = Some(Price::try_new(101).unwrap());
                    p.new_tif = Some(gtd(40));
                } else {
                    p.new_market_protection = Some(MarketProtectionPatch::Set(MarketProtection {
                        max_slippage_bps: Some(50),
                        worst_price: Some(Price::try_new(120).unwrap()),
                    }));
                }
                ok(&mut h, amend(p));
            }
            for expiry in [
                TriggerExpiryPatch::Keep,
                TriggerExpiryPatch::ClearToGtc,
                TriggerExpiryPatch::Set(TriggerExpiry::Day {
                    session_id: SESSION,
                }),
                TriggerExpiryPatch::Set(TriggerExpiry::Gtd {
                    expire_at: LogicalTime::new(20),
                }),
            ] {
                let mut p = patch(1);
                p.new_trigger_expiry = Some(expiry);
                ok(&mut h, amend(p));
            }
            let mut p = patch(1);
            p.new_stop_price =
                Some(Price::try_new(if side == Side::Buy { 106 } else { 94 }).unwrap());
            let mut target = patch(2);
            target.new_stop_price = p.new_stop_price;
            ok(&mut h, amend(target));
            ok(&mut h, amend(p));
            assert_eq!(
                h.p.stop_orders(side)
                    .iter()
                    .map(|(o, _)| o.order_id().get())
                    .collect::<Vec<_>>(),
                [2, 1]
            );
            for reference in [
                PriceReferenceKind::MarkPrice,
                PriceReferenceKind::IndexPrice,
                PriceReferenceKind::LastTrade,
            ] {
                let mut p = patch(1);
                p.new_trigger_reference = Some(reference);
                ok(&mut h, amend(p));
            }
            for protection in [MarketProtectionPatch::Keep, MarketProtectionPatch::Clear] {
                if kind == 2 {
                    continue;
                }
                let mut p = patch(1);
                p.new_market_protection = Some(protection);
                ok(&mut h, amend(p));
            }
            let mut p = patch(1);
            if kind == 3 {
                p.new_qty = Some(Qty::try_new(2).unwrap());
            } else {
                p.new_quote_budget = Some(QuoteAmount::new(100));
            }
            reject(&mut h, amend(p), RejectReason::InvalidOrderCombination);
            let mut p = patch(1);
            p.new_qty = Some(Qty::try_new(2).unwrap());
            p.new_quote_budget = Some(QuoteAmount::new(100));
            reject(&mut h, amend(p), RejectReason::InvalidOrderCombination);
            ok(&mut h, Command::AdvanceTime(LogicalTime::new(20)));
            assert!(h.p.stop_order(OrderId::new(1)).is_none());
            assert!(h.p.stop_order(OrderId::new(2)).is_some());
        }
    }
}

#[test]
fn amend_full_fixed_iceberg_reduce_exposure_and_signed_versions_each_container() {
    for side in [Side::Buy, Side::Sell] {
        for kind in 0..5 {
            let mut h = Harness::new();
            initialize(&mut h, 1, kind, side);
            let mut p = patch(1);
            if kind == 3 {
                p.new_quote_budget = Some(QuoteAmount::new(900));
            } else {
                p.new_qty = Some(Qty::try_new(9).unwrap());
            }
            if kind == 1 {
                p.new_visible_qty = Some(Qty::try_new(2).unwrap());
            }
            ok(&mut h, amend(p));
            if kind == 1 {
                let mut exposure = patch(1);
                exposure.new_visible_qty = Some(Qty::try_new(4).unwrap());
                let before = h.p.order(OrderId::new(1)).unwrap().priority();
                let record = ok(&mut h, amend(exposure));
                let iceberg = record.state.regular(OrderId::new(1)).unwrap();
                assert_eq!((iceberg.total, iceberg.visible), (9, 4));
                assert!(iceberg.priority > before);
            }
            let mut p = patch(1);
            if kind == 3 {
                p.new_quote_budget = Some(QuoteAmount::new(1500));
            } else {
                p.new_qty = Some(Qty::try_new(15).unwrap());
            }
            if kind == 1 {
                p.new_visible_qty = Some(Qty::try_new(5).unwrap());
            }
            signed(&mut p, 81, 11);
            let command = Command::Amend {
                patch: p.into(),
                command_id: 81,
                required: 11_000,
            };
            ok(&mut h, command.clone());
            assert_eq!(h.p.reservation_state(OrderId::new(1)).unwrap().version, 12);
            reject(&mut h, command, RejectReason::InvalidReservation);
            let mut p = patch(1);
            signed(&mut p, 82, 12);
            ok(
                &mut h,
                Command::Amend {
                    patch: p.into(),
                    command_id: 82,
                    required: 11_000,
                },
            );
            assert_eq!(h.p.reservation_state(OrderId::new(1)).unwrap().version, 13);
        }
    }
}

#[test]
fn amend_full_fixed_sources_trigger_amended_child_and_reservation_lifecycle() {
    for side in [Side::Buy, Side::Sell] {
        let mut h = Harness::new();
        initialize(&mut h, 1, 2, side);
        let mut p = patch(1);
        p.new_trigger_reference = Some(PriceReferenceKind::MarkPrice);
        p.new_price = Some(Price::try_new(100).unwrap());
        p.new_qty = Some(Qty::try_new(8).unwrap());
        signed(&mut p, 80, 11);
        ok(&mut h, amend(p));
        ok(
            &mut h,
            Command::Rest(order(2, opposite_side(side), 100, 3), TimeInForce::Gtc),
        );
        for kind in [
            PriceReferenceKind::LastTrade,
            PriceReferenceKind::IndexPrice,
            PriceReferenceKind::MarkPrice,
        ] {
            ok(
                &mut h,
                Command::Reference(PriceReference {
                    kind,
                    source_seq: 7,
                    price: Price::try_new(100).unwrap(),
                }),
            );
        }
        let trigger = PriceReference {
            kind: PriceReferenceKind::MarkPrice,
            source_seq: 8,
            price: Price::try_new(if side == Side::Buy { 110 } else { 90 }).unwrap(),
        };
        let result = ok(&mut h, Command::Reference(trigger));
        assert!(matches!(
            result.outcome.events().first(),
            Some(EngineEvent::StopTriggered { .. })
        ));
        let child = result.state.regular(OrderId::new(1)).unwrap();
        assert_eq!((child.original.qty.get(), child.total), (8, 5));
        assert_eq!(
            result.state.reservation(OrderId::new(1)).unwrap().version,
            12
        );
        let mut p = patch(1);
        p.new_qty = Some(Qty::try_new(6).unwrap());
        ok(&mut h, amend(p));
        reject(
            &mut h,
            Command::Reference(trigger),
            RejectReason::StaleReferencePriceUpdate,
        );
        ok(&mut h, Command::Cancel(OrderId::new(1)));
        assert!(h.p.reservation_state(OrderId::new(1)).is_none());
    }
}

proptest! {
    #![proptest_config(Config { cases: 48, rng_seed: proptest::test_runner::RngSeed::Fixed(0xA12_5ACC), failure_persistence: None, ..Config::default() })]
    #[test]
    fn amend_full_success_properties_generated_matrix(side in any::<bool>(), kind in 0u8..5, total in 8u64..25, peak in 1u64..6) {
        let side = if side { Side::Buy } else { Side::Sell };
        let mut h = Harness::new();
        initialize(&mut h, 1, kind, side);
        let mut p = patch(1);
        if kind == 3 { p.new_quote_budget = Some(QuoteAmount::new(u128::from(total) * 100)); }
        else { p.new_qty = Some(Qty::try_new(total).unwrap()); }
        if kind == 1 { p.new_visible_qty = Some(Qty::try_new(peak).unwrap()); }
        ok(&mut h, amend(p));
        let mut p = patch(1); signed(&mut p, 80, 11);
        let command = amend(p); ok(&mut h, command.clone());
        reject(&mut h, command, RejectReason::InvalidReservation);
        let mut p = patch(1);
        if kind < 2 { p.new_price = Some(Price::try_new(if side == Side::Buy { 89 } else { 111 }).unwrap()); }
        else { p.new_stop_price = Some(Price::try_new(102).unwrap()); p.new_trigger_reference = Some(PriceReferenceKind::IndexPrice); }
        ok(&mut h, amend(p));
    }
}
