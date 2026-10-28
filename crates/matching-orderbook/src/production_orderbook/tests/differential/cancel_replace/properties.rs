//! 从独立输入推导成功后置条件；短状态机补充长磁带的身份、归属和优先级性质。
use super::*;

fn install_old(h: &mut Harness, kind: u8, side: Side, price: i64, qty: u64) {
    let command = match kind {
        0 => Command::Rest(input(1, side, price, qty), gtd(40)),
        1 => Command::Iceberg(
            IcebergOrder::try_new(input(1, side, price, qty), Qty::try_new(1).unwrap()).unwrap(),
        ),
        2 => Command::Stop(scenarios::stop(1, side, 40)),
        _ => unreachable!(),
    };
    assert!(!h.run(&command).outcome.failed());
}

fn absent(h: &Harness, id: u128) {
    let id = OrderId::new(id);
    for side in [Side::Buy, Side::Sell] {
        assert!(
            h.r.orders(side)
                .iter()
                .all(|o| o.original_order().order_id != id)
        );
    }
    assert!(h.r.stop_order(id).is_none());
    assert!(h.r.reservation_state(id).is_none());
    assert!(h.p.order(id).is_none());
    assert!(h.p.stop_order(id).is_none());
    assert!(h.p.reservation_state(id).is_none());
}

fn canceled_once(events: &[EngineEvent], id: u128) {
    assert_eq!(events.iter().filter(|event| matches!(event,
        EngineEvent::OrderCanceled { order_id, reason: matching_domain::CancelReason::CancelReplace, .. }
        if *order_id == OrderId::new(id))).count(), 1);
    assert!(
        matches!(events.first(), Some(EngineEvent::OrderCanceled {order_id, ..})
        if *order_id == OrderId::new(id))
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, EngineEvent::ReservationReleased { .. }))
    );
}

/// 预期金融载荷只由输入及已冻结的三种过渡规则推导，绝不复制模型结果。
fn reservation_after(old: u128, new: u128, transition: u8) -> ReservationState {
    if transition == 2 {
        let mut expected = shadow(new);
        expected.version = 0;
        expected.reserves = reserves(12_000);
        expected
    } else {
        let mut expected = shadow(old);
        expected.order_id = OrderId::new(new);
        expected.version = 4;
        expected.reserves = reserves(if transition == 0 { 9000 } else { 12_000 });
        expected
    }
}

fn binding(h: &Harness, expected: &ReservationState, pending: bool) {
    for registry in [&h.rr, &h.pr] {
        assert_eq!(registry.get(expected.reservation_id), Some(expected));
        assert_eq!(registry.is_pending_r2(expected.reservation_id), pending);
    }
    for actual in [
        h.r.reservation_state(expected.order_id),
        h.p.reservation_state(expected.order_id),
    ] {
        assert_eq!(actual, if pending { None } else { Some(expected) });
    }
}

fn fok_case(buy: bool, old_kind: u8, iceberg: bool, transition: u8, liquid: u64, requested: u64) {
    let side = if buy { Side::Buy } else { Side::Sell };
    let maker_side = opposite_side(side);
    let mut h = Harness::new();
    install_old(&mut h, old_kind, maker_side, 100, 12);
    let maker = input(2, maker_side, 100, liquid);
    h.run(&if iceberg {
        Command::Iceberg(IcebergOrder::try_new(maker, Qty::try_new(1).unwrap()).unwrap())
    } else {
        Command::Rest(maker, TimeInForce::Gtc)
    });
    h.run(&Command::Rest(
        input(3, maker_side, if buy { 101 } else { 99 }, 12),
        TimeInForce::Gtc,
    ));
    let before = h.p.next_queue_priority();
    let command = replacement(
        (1, 9, 1, 3),
        41,
        transition,
        side,
        100,
        requested,
        limit(TimeInForce::Fok, false),
    );
    let record = h.run(&command);
    if requested > liquid {
        let Command::CancelReplace { request, .. } = command else {
            unreachable!()
        };
        assert_eq!(
            record.outcome,
            Outcome::CancelReplace(Err(
                request.rejection(RejectReason::FokInsufficientLiquidity)
            ))
        );
        assert_eq!(h.p.next_queue_priority(), before);
        return;
    }
    assert!(!record.outcome.failed());
    let events = record.outcome.events();
    canceled_once(events, 1);
    let trades = events
        .iter()
        .filter_map(|event| {
            if let EngineEvent::Trade(t) = event {
                Some(t)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(trades.iter().map(|t| t.qty.get()).sum::<u64>(), requested);
    assert!(trades.iter().all(|t| t.maker_order_id == OrderId::new(2)
        && t.taker_order_id == OrderId::new(9)
        && t.price == Price::try_new(100).unwrap()));
    absent(&h, 1);
    absent(&h, 9);
    binding(&h, &reservation_after(1, 9, transition), true);
    if transition == 2 {
        binding(&h, &shadow(1), true);
    }
    binding(&h, &shadow(2), requested == liquid);
    binding(&h, &shadow(3), false);
    // 只有真实冰山补量可推进计数器；FOK 新单本身永不取得队列优先级。
    let refreshes = events
        .iter()
        .filter(|e| matches!(e, EngineEvent::IcebergRefreshed { .. }))
        .count() as u128;
    assert_eq!(h.r.test_next_queue_priority(), before + refreshes);
    assert_eq!(h.p.next_queue_priority(), before + refreshes);
}

fn post_only_case(
    buy: bool,
    old_kind: u8,
    iceberg: bool,
    transition: u8,
    qty: u64,
    crossing: bool,
) {
    let side = if buy { Side::Buy } else { Side::Sell };
    let maker_side = opposite_side(side);
    let best = if buy { 101 } else { 99 };
    let price = if crossing { best } else { 100 };
    let mut h = Harness::new();
    install_old(&mut h, old_kind, maker_side, 100, 12);
    let other = input(2, maker_side, best, 12);
    h.run(&Command::Rest(other.clone(), TimeInForce::Gtc));
    let kind = if iceberg {
        ReplacementOrderKind::Iceberg {
            peak_size: Qty::try_new(1).unwrap(),
            tif: TimeInForce::Gtc,
            flags: flags(true),
        }
    } else {
        limit(TimeInForce::Gtc, true)
    };
    // 两个全新普通 Place 簿仅含排除 old 后的真实 maker，独立验证提交后的交叉条件。
    let mut normal_r = ReferenceOrderBook::new();
    let mut normal_p = ProductionOrderBook::new(policy());
    normal_r.rest(other.clone()).unwrap();
    normal_p.rest(other).unwrap();
    let new = input(9, side, price, qty);
    let normal = if iceberg {
        let new = IcebergOrder::try_new(new, Qty::try_new(1).unwrap()).unwrap();
        (
            normal_r.place_iceberg_post_only(new.clone(), &math()),
            normal_p.place_iceberg_post_only(new, &math()),
        )
    } else {
        (
            normal_r.place_limit_post_only(new.clone()),
            normal_p.place_limit_post_only(new),
        )
    };
    let expected = if crossing {
        Err(RejectReason::PostOnlyWouldCross)
    } else {
        Ok(())
    };
    assert_eq!(normal.0, expected);
    assert_eq!(normal.1, expected);
    let before = h.p.next_queue_priority();
    let command = replacement((1, 9, 1, 3), 41, transition, side, price, qty, kind);
    let record = h.run(&command);
    if crossing {
        let Command::CancelReplace { request, .. } = command else {
            unreachable!()
        };
        assert_eq!(
            record.outcome,
            Outcome::CancelReplace(Err(request.rejection(RejectReason::PostOnlyWouldCross)))
        );
    } else {
        assert!(!record.outcome.failed());
        canceled_once(record.outcome.events(), 1);
        assert_eq!(record.outcome.events().len(), 1);
        absent(&h, 1);
        for priority in [
            h.r.orders(side)
                .iter()
                .find(|o| o.original_order().order_id == OrderId::new(9))
                .unwrap()
                .priority(),
            h.p.order(OrderId::new(9)).unwrap().priority(),
        ] {
            assert_eq!(priority.get(), before);
        }
        assert_eq!(h.p.next_queue_priority(), before + 1);
        binding(&h, &reservation_after(1, 9, transition), false);
        if transition == 2 {
            binding(&h, &shadow(1), true);
        }
        binding(&h, &shadow(2), false);
    }
}

fn priority_case(buy: bool, old_kind: u8, transition: u8, mode: u8, qty: u64) {
    let side = if buy { Side::Buy } else { Side::Sell };
    let rest_price = if buy { 90 } else { 110 };
    let mut h = Harness::new();
    if old_kind == 2 {
        // 刻意使旧 StopPriority 与 regular 分配器分离，不能靠数值巧合掩盖转移。
        for id in 10..14 {
            h.run(&Command::Stop(scenarios::stop(id, side, 40)));
        }
    }
    install_old(&mut h, old_kind, side, rest_price, 12);
    h.run(&Command::Rest(
        input(2, opposite_side(side), 100, qty),
        TimeInForce::Gtc,
    ));
    h.run(&Command::Rest(
        input(3, side, rest_price, 12),
        TimeInForce::Gtc,
    ));
    let next = h.p.next_queue_priority();
    let stop_next = h.p.next_stop_priority();
    let old_priority = if old_kind == 2 {
        h.p.stop_order(OrderId::new(1)).unwrap().1.get()
    } else {
        h.p.order(OrderId::new(1)).unwrap().priority().get()
    };
    let (price, requested, tif, rests) = match mode {
        0 => (rest_price, qty, TimeInForce::Gtc, true),
        1 => (100, qty, TimeInForce::Fok, false),
        2 => (100, qty + 1, TimeInForce::Gtc, true),
        3 => (100, qty + 1, TimeInForce::Ioc, false),
        4 => (rest_price, qty, TimeInForce::Ioc, false),
        5 => (100, qty, TimeInForce::Gtc, false),
        _ => unreachable!(),
    };
    let record = h.run(&replacement(
        (1, 9, 1, 3),
        41,
        transition,
        side,
        price,
        requested,
        limit(tif, false),
    ));
    assert!(!record.outcome.failed());
    canceled_once(record.outcome.events(), 1);
    absent(&h, 1);
    assert_eq!(h.r.test_next_queue_priority(), next + u128::from(rests));
    assert_eq!(h.p.next_queue_priority(), next + u128::from(rests));
    assert_eq!(h.r.next_stop_priority(), stop_next);
    assert_eq!(h.p.next_stop_priority(), stop_next);
    if rests {
        let reference = h.r.orders(side);
        let reference = reference
            .iter()
            .find(|o| o.original_order().order_id == OrderId::new(9))
            .unwrap();
        let production = h.p.order(OrderId::new(9)).unwrap();
        assert_eq!(reference.priority().get(), next);
        assert_eq!(production.priority().get(), next);
        assert_ne!(next, old_priority);
        let remaining = if mode == 2 { 1 } else { qty };
        assert_eq!(reference.total_remaining(), remaining);
        assert_eq!(production.total_remaining(), remaining);
        if mode == 0 {
            let ids =
                h.r.orders(side)
                    .iter()
                    .map(|o| o.original_order().order_id)
                    .collect::<Vec<_>>();
            assert_eq!(ids, vec![OrderId::new(3), OrderId::new(9)]);
        }
    } else {
        absent(&h, 9);
    }
    binding(&h, &reservation_after(1, 9, transition), !rests);
    if transition == 2 {
        binding(&h, &shadow(1), true);
    }
}

/// 输入序列在执行前完整确定，成功与拒绝交错时也不从模型状态反向修补版本或签名。
fn lifecycle_tape(buy: bool, qty: u64) -> Vec<Command> {
    let side = if buy { Side::Buy } else { Side::Sell };
    let resting = if buy { 90 } else { 110 };
    let outside = if buy { 101 } else { 99 };
    let distant = if buy { 80 } else { 120 };
    let mut bad_adjustment = replacement(
        (9, 10, 1, 4),
        44,
        1,
        side,
        resting,
        qty,
        limit(gtd(50), false),
    );
    let Command::CancelReplace { request, .. } = &mut bad_adjustment else {
        unreachable!()
    };
    let ReservationTransition::ReuseOld {
        adjustment: Some(token),
    } = &mut request.reservation_transition
    else {
        unreachable!()
    };
    token.signature[0] ^= 1;
    let mut bad_new = replacement(
        (10, 11, 1, 5),
        46,
        2,
        side,
        100,
        qty + 1,
        limit(gtd(60), false),
    );
    let Command::CancelReplace { expected, .. } = &mut bad_new else {
        unreachable!()
    };
    expected.required_reserves[0].atoms = 20_000;
    vec![
        Command::Rest(input(2, opposite_side(side), 100, qty), TimeInForce::Gtc),
        Command::Rest(
            input(3, opposite_side(side), outside, qty),
            TimeInForce::Gtc,
        ),
        Command::Rest(input(4, side, distant, qty), gtd(40)),
        Command::Rest(
            input(5, side, distant, qty),
            TimeInForce::Day {
                session_id: SESSION,
            },
        ),
        Command::Stop(scenarios::stop(1, side, 40)),
        replacement(
            (1, 9, 1, 3),
            41,
            0,
            side,
            100,
            qty + 1,
            limit(TimeInForce::Fok, false),
        ),
        replacement(
            (1, 9, 1, 3),
            42,
            0,
            side,
            resting,
            12,
            limit(TimeInForce::Gtc, true),
        ),
        replacement(
            (1, 14, 1, 3),
            43,
            2,
            side,
            resting,
            qty,
            limit(TimeInForce::Gtc, false),
        ),
        Command::Amend(Box::new(AmendOrder {
            market_id: MarketId::new(7),
            user_id: UserId::new(5),
            order_id: OrderId::new(9),
            new_price: None,
            new_qty: Some(Qty::try_new(qty).unwrap()),
            new_quote_budget: None,
            new_visible_qty: None,
            new_stop_price: None,
            new_trigger_reference: None,
            new_trigger_expiry: None,
            new_tif: None,
            new_market_protection: None,
            reservation_adjustment: None,
        })),
        bad_adjustment,
        replacement(
            (9, 10, 1, 4),
            45,
            1,
            side,
            resting,
            qty,
            limit(gtd(50), false),
        ),
        bad_new,
        replacement(
            (10, 11, 1, 5),
            47,
            2,
            side,
            100,
            qty + 1,
            limit(gtd(60), false),
        ),
        replacement(
            (10, 14, 1, 5),
            48,
            2,
            side,
            resting,
            qty,
            limit(TimeInForce::Gtc, false),
        ),
        replacement(
            (11, 12, 11, 0),
            49,
            0,
            side,
            outside,
            qty,
            limit(TimeInForce::Fok, false),
        ),
        Command::AdvanceTime(LogicalTime::new(60)),
        Command::EndSession(SESSION),
        Command::Cancel(OrderId::new(9)),
        replacement(
            (12, 15, 11, 1),
            50,
            2,
            side,
            resting,
            qty,
            limit(TimeInForce::Gtc, false),
        ),
    ]
}

fn lifecycle_case(buy: bool, qty: u64) {
    let tape = lifecycle_tape(buy, qty);
    assert_eq!(tape, lifecycle_tape(buy, qty));
    let mut h = Harness::new();
    let mut replay = Harness::new();
    let mut previous = (h.p.next_queue_priority(), h.p.next_stop_priority());
    let mut terminal_old = shadow(1);
    terminal_old.order_id = OrderId::new(10);
    terminal_old.version = 5;
    terminal_old.reserves = reserves(12_000);
    for (step, command) in tape.iter().enumerate() {
        let record = h.run(command);
        assert_eq!(
            record,
            replay.run(command),
            "step={step} command={command:?}"
        );
        let current = (h.p.next_queue_priority(), h.p.next_stop_priority());
        assert!(current.0 >= previous.0 && current.1 >= previous.1);
        previous = current;
        match step {
            5 | 7 | 9 | 11 | 13 | 18 => {
                let Command::CancelReplace { request, .. } = command else {
                    unreachable!()
                };
                let reason = match step {
                    5 => RejectReason::FokInsufficientLiquidity,
                    9 => RejectReason::InvalidReservation,
                    11 => RejectReason::InsufficientReservation,
                    _ => RejectReason::OrderNotFound,
                };
                assert_eq!(
                    record.outcome,
                    Outcome::CancelReplace(Err(request.rejection(reason)))
                );
                if let ReservationTransition::ReplaceWithNew { new_reservation } =
                    &request.reservation_transition
                {
                    assert!(h.rr.get(new_reservation.reservation_id).is_none());
                    assert!(h.pr.get(new_reservation.reservation_id).is_none());
                }
            }
            6 => {
                canceled_once(record.outcome.events(), 1);
                absent(&h, 1);
                binding(&h, &reservation_after(1, 9, 0), false);
            }
            10 => {
                canceled_once(record.outcome.events(), 9);
                absent(&h, 9);
                binding(&h, &terminal_old, false);
            }
            12 => {
                canceled_once(record.outcome.events(), 10);
                absent(&h, 10);
                binding(&h, &terminal_old, true);
                binding(&h, &reservation_after(10, 11, 2), false);
                binding(&h, &shadow(2), true);
            }
            14 => {
                canceled_once(record.outcome.events(), 11);
                absent(&h, 11);
                absent(&h, 12);
                let mut terminal = reservation_after(10, 11, 2);
                terminal.order_id = OrderId::new(12);
                terminal.version = 1;
                terminal.reserves = reserves(9000);
                binding(&h, &terminal, true);
                binding(&h, &terminal_old, true);
                binding(&h, &shadow(3), true);
                assert_eq!(
                    record
                        .outcome
                        .events()
                        .iter()
                        .filter_map(|e| if let EngineEvent::Trade(t) = e {
                            Some(t.qty.get())
                        } else {
                            None
                        })
                        .sum::<u64>(),
                    qty
                );
            }
            15 | 16 => {
                let Outcome::Expired(Ok(expired)) = &record.outcome else {
                    panic!("缺少到期结果")
                };
                assert_eq!(
                    expired.iter().map(|o| o.order_id).collect::<Vec<_>>(),
                    vec![OrderId::new(if step == 15 { 4 } else { 5 })]
                );
            }
            17 => assert_eq!(
                record.outcome,
                Outcome::Unit(Err(RejectReason::OrderNotFound))
            ),
            _ => assert!(!record.outcome.failed()),
        }
    }
    assert_eq!(h.known_r.len(), 6);
    assert_eq!(h.known_p.len(), 6);
    for id in [1, 2, 3, 4, 5, 11] {
        assert!(h.rr.is_pending_r2(ReservationId::new(id)));
        assert!(h.pr.is_pending_r2(ReservationId::new(id)));
    }
    h.observe();
}

#[test]
fn cancel_replace_properties_fixed_mixed_lifecycle_repeats_exact_transcript() {
    for buy in [false, true] {
        for qty in [2, 9] {
            lifecycle_case(buy, qty);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        rng_algorithm: RngAlgorithm::ChaCha,
        rng_seed: proptest::test_runner::RngSeed::Fixed(0x13_10_C3_49_B6),
        ..ProptestConfig::default()
    })]

    #[test]
    fn cancel_replace_properties_fok_all_or_zero_without_old(buy in any::<bool>(), old_kind in 0u8..3, iceberg in any::<bool>(), transition in 0u8..3, liquid in 2u64..14, requested in 1u64..26) {
        fok_case(buy, old_kind, iceberg, transition, liquid, requested);
    }

    #[test]
    fn cancel_replace_properties_post_only_matches_post_removal_place_and_never_takes(buy in any::<bool>(), old_kind in 0u8..2, iceberg in any::<bool>(), transition in 0u8..3, qty in 2u64..26, crossing in any::<bool>()) {
        post_only_case(buy, old_kind, iceberg, transition, qty, crossing);
    }

    #[test]
    fn cancel_replace_properties_new_priority_only_on_actual_rest(buy in any::<bool>(), old_kind in 0u8..3, transition in 0u8..3, mode in 0u8..6, qty in 2u64..14) {
        priority_case(buy, old_kind, transition, mode, qty);
    }

    #[test]
    fn cancel_replace_properties_mixed_lifecycle_failure_atomicity_and_repeatability(buy in any::<bool>(), qty in 2u64..10) {
        lifecycle_case(buy, qty);
    }
}
