//! 固定种子连续混合磁带；区块只清理活动簿，不重置登记历史、时钟或身份源。
use super::*;

const GENERATOR: &str = "cancel-replace-mixed-v1-ChaCha";
const SEED_100K: [u8; 32] = [0xC3; 32];
const SEED_A: [u8; 32] = [0x49; 32];
const SEED_B: [u8; 32] = [0xB6; 32];

fn reduction(id: u128) -> Command {
    Command::Amend(Box::new(AmendOrder {
        market_id: MarketId::new(7),
        user_id: UserId::new(5),
        order_id: OrderId::new(id),
        new_price: None,
        new_qty: Some(Qty::try_new(9).unwrap()),
        new_quote_budget: None,
        new_visible_qty: None,
        new_stop_price: None,
        new_trigger_reference: None,
        new_trigger_expiry: None,
        new_tif: None,
        new_market_protection: None,
        reservation_adjustment: None,
    }))
}

fn generate(seed: [u8; 32], length: usize) -> Vec<Command> {
    assert!(length <= 100_000);
    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &seed);
    let mut runner = TestRunner::new_with_rng(Config::default(), rng);
    let strategy = (0u8..16, 0u8..6, any::<bool>());
    let mut tape = Vec::with_capacity(length);
    let mut block = 0u64;
    while tape.len() < length {
        let base = u128::from(block) * 10_000 + 1;
        let now = 1 + block * 10;
        let mut batch = vec![
            Command::Rest(input(base, Side::Buy, 90, 12), gtd(now + 8)),
            Command::Iceberg(
                IcebergOrder::try_new(
                    input(base + 1, Side::Sell, 110, 12),
                    Qty::try_new(3).unwrap(),
                )
                .unwrap(),
            ),
            Command::Stop(scenarios::stop(base + 2, Side::Buy, now + 8)),
            Command::Stop(scenarios::stop(base + 3, Side::Sell, now + 8)),
            Command::Rest(input(base + 4, Side::Sell, 100, 6), TimeInForce::Gtc),
            Command::Iceberg(
                IcebergOrder::try_new(
                    input(base + 5, Side::Sell, 101, 12),
                    Qty::try_new(3).unwrap(),
                )
                .unwrap(),
            ),
            Command::Rest(input(base + 6, Side::Buy, 99, 6), TimeInForce::Gtc),
            Command::Rest(input(base + 7, Side::Buy, 98, 6), TimeInForce::Gtc),
            Command::Rest(
                input(base + 8, Side::Buy, 88, 12),
                TimeInForce::Day {
                    session_id: SESSION,
                },
            ),
            Command::Rest(input(base + 9, Side::Sell, 112, 12), gtd(now + 8)),
        ];
        for source in [
            PriceReferenceKind::LastTrade,
            PriceReferenceKind::MarkPrice,
            PriceReferenceKind::IndexPrice,
        ] {
            batch.push(Command::Reference(PriceReference {
                kind: source,
                source_seq: block * 10_000 + 1,
                price: Price::try_new(100).unwrap(),
            }));
        }
        // 独立来源命令：冰山旧单转 FOK 全成，两个止损旧单转普通/冰山实际挂单。
        batch.push(replacement(
            (base + 1, base + 50, base + 1, 3),
            base + 50,
            2,
            Side::Buy,
            101,
            18,
            limit(TimeInForce::Fok, false),
        ));
        batch.push(replacement(
            (base + 2, base + 51, base + 2, 3),
            base + 51,
            2,
            Side::Buy,
            90,
            12,
            limit(TimeInForce::Gtc, false),
        ));
        batch.push(replacement(
            (base + 3, base + 52, base + 3, 3),
            base + 52,
            1,
            Side::Sell,
            110,
            12,
            ReplacementOrderKind::Iceberg {
                peak_size: Qty::try_new(3).unwrap(),
                tif: TimeInForce::Gtc,
                flags: flags(true),
            },
        ));
        let mut old = base;
        for (version, offset) in (3u64..).zip(0u128..400) {
            let (bad, kind, buy) = strategy.new_tree(&mut runner).unwrap().current();
            let new = base + 100 + offset;
            let side = if buy { Side::Buy } else { Side::Sell };
            let price = if buy { 90 } else { 110 };
            let kind = if offset == 399 { 0 } else { kind };
            let kind = match kind {
                0 => limit(TimeInForce::Gtc, false),
                1 => limit(gtd(now + 8), false),
                2 => limit(
                    TimeInForce::Day {
                        session_id: SESSION,
                    },
                    false,
                ),
                3 => limit(TimeInForce::Gtc, true),
                4 | 5 => ReplacementOrderKind::Iceberg {
                    peak_size: Qty::try_new(3).unwrap(),
                    tif: TimeInForce::Gtc,
                    flags: flags(kind == 5),
                },
                _ => unreachable!(),
            };
            let mut invalid = replacement(
                (old, new, base, version),
                new * 2,
                0,
                side,
                price,
                12,
                kind.clone(),
            );
            // 每条随机拒绝都在确定性成功链之前，不能改变之后命令的预先签名身份和版本。
            let Command::CancelReplace { request, expected } = &mut invalid else {
                unreachable!()
            };
            match bad {
                0 => request.user_id = UserId::new(6),
                1 => request.replacement.order_id = request.old_order_id,
                2 => request.replacement.kind = limit(gtd(now), false),
                3 => {
                    request.replacement.kind = limit(
                        TimeInForce::Day {
                            session_id: SESSION - 1,
                        },
                        false,
                    )
                }
                4 => request.replacement.kind = limit(TimeInForce::Ioc, true),
                5 => expected.expected_version += 1,
                6 => expected.required_reserves[0].atoms = 20_000,
                7 => request.replacement.user_id = UserId::new(6),
                8 => request.replacement.market_id = MarketId::new(8),
                9 => request.replacement.order_id = OrderId::new(base + 8),
                10 => {
                    request.replacement.kind = ReplacementOrderKind::Iceberg {
                        peak_size: request.replacement.qty,
                        tif: TimeInForce::Gtc,
                        flags: flags(false),
                    }
                }
                11 => request.old_order_id = OrderId::new(base + 9999),
                12 => expected.required_reserves.clear(),
                13 => {
                    request.replacement.kind = ReplacementOrderKind::Iceberg {
                        peak_size: Qty::try_new(3).unwrap(),
                        tif: TimeInForce::Ioc,
                        flags: flags(false),
                    }
                }
                14 => {
                    request.replacement.kind = limit(TimeInForce::Gtc, false);
                    if let ReplacementOrderKind::Limit { flags, .. } = &mut request.replacement.kind
                    {
                        flags.reduce_only = true;
                    }
                }
                15 => expected.old_reservation_id = ReservationId::new(base + 9999),
                _ => unreachable!(),
            }
            batch.push(invalid);
            batch.push(replacement(
                (old, new, base, version),
                new * 2 + 1,
                u8::from(offset == 0),
                side,
                price,
                12,
                kind,
            ));
            old = new;
        }
        batch.push(reduction(base + 8));
        batch.push(reduction(old));
        batch.push(Command::Place(input(base + 5000, Side::Buy, 99, 12)));
        batch.push(Command::Place(input(base + 5001, Side::Sell, 99, 18)));
        batch.push(Command::Rest(
            input(base + 5002, Side::Sell, 112, 12),
            gtd(now + 5),
        ));
        batch.push(Command::Rest(
            input(base + 5003, Side::Buy, 88, 12),
            TimeInForce::Day {
                session_id: SESSION,
            },
        ));
        batch.push(Command::AdvanceTime(LogicalTime::new(now + 8)));
        batch.push(Command::EndSession(SESSION));
        for id in (base..base + 10)
            .chain([base + 50, base + 51, base + 52, old])
            .chain(base + 5000..base + 5004)
        {
            batch.push(Command::Cancel(OrderId::new(id)));
        }
        while batch.len() < 1023 {
            let (bad, _, _) = strategy.new_tree(&mut runner).unwrap().current();
            batch.push(if bad % 2 == 0 {
                Command::Cancel(OrderId::new(base))
            } else {
                Command::EndSession(SESSION - 1)
            });
        }
        batch.push(Command::AdvanceTime(LogicalTime::new(now + 10)));
        assert_eq!(batch.len(), 1024);
        tape.extend(batch.into_iter().take(length - tape.len()));
        block += 1;
    }
    tape
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Coverage {
    /// 已执行的总命令数。
    commands: usize,
    /// 成功提交的撤单替换次数。
    successful_replace: usize,
    /// 被拒绝且保持原子性的撤单替换次数。
    rejected_replace: usize,
    /// 成功延续旧预留且未调整金额的次数。
    reuse: usize,
    /// 成功延续旧预留并应用调整授权的次数。
    adjusted: usize,
    /// 成功接管独立新预留的次数。
    new_reservation: usize,
    /// 成功替换的旧冰山订单次数。
    old_iceberg: usize,
    /// 成功替换的旧止损限价订单次数。
    old_stop: usize,
    /// 全部命令累计产生的成交数量。
    trades: usize,
    /// 成功 Amend 命令次数。
    amended: usize,
    /// 成功 Cancel 命令次数。
    canceled: usize,
    /// 成功 expiry 命令次数。
    expired: usize,
    /// 成功参考价更新命令次数。
    references: usize,
}

impl Coverage {
    fn record(&mut self, command: &Command, before: (bool, bool), result: &Record) {
        self.commands += 1;
        match command {
            Command::CancelReplace { request, .. } => {
                if result.outcome.failed() {
                    self.rejected_replace += 1;
                } else {
                    self.successful_replace += 1;
                    self.old_iceberg += usize::from(before.0);
                    self.old_stop += usize::from(before.1);
                    match &request.reservation_transition {
                        ReservationTransition::ReuseOld { adjustment: None } => self.reuse += 1,
                        ReservationTransition::ReuseOld {
                            adjustment: Some(_),
                        } => self.adjusted += 1,
                        ReservationTransition::ReplaceWithNew { .. } => self.new_reservation += 1,
                    }
                }
            }
            Command::Amend(_) if !result.outcome.failed() => self.amended += 1,
            Command::Cancel(_) if !result.outcome.failed() => self.canceled += 1,
            Command::Reference(_) if !result.outcome.failed() => self.references += 1,
            _ => {}
        }
        self.trades += result
            .outcome
            .events()
            .iter()
            .filter(|e| matches!(e, EngineEvent::Trade(_)))
            .count();
        if let Outcome::Expired(Ok(expired)) = &result.outcome {
            self.expired += expired.len();
        }
    }

    fn complete(&self, length: usize) {
        assert_eq!(self.commands, length);
        assert!(self.successful_replace > 0 && self.rejected_replace > 0);
        assert!(self.reuse > 0 && self.adjusted > 0 && self.new_reservation > 0);
        assert!(self.old_iceberg > 0 && self.old_stop > 0);
        assert!(
            self.trades > 0
                && self.amended > 0
                && self.canceled > 0
                && self.expired > 0
                && self.references > 0
        );
    }
}

fn old_kind(h: &Harness, command: &Command) -> (bool, bool) {
    if let Command::CancelReplace { request, .. } = command {
        (
            h.p.order(request.old_order_id)
                .is_some_and(|o| o.peak_size().is_some()),
            h.p.stop_order(request.old_order_id).is_some(),
        )
    } else {
        (false, false)
    }
}

fn execute(h: &mut Harness, command: &Command, seed: [u8; 32], step: usize) -> Record {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| h.run(command))).unwrap_or_else(
        |error| {
            eprintln!("{GENERATOR} seed={seed:02x?} step={step} command={command:?}");
            std::panic::resume_unwind(error)
        },
    )
}

fn run(seed: [u8; 32], length: usize, repeat: bool) -> Coverage {
    let commands = generate(seed, length);
    assert_eq!(commands, generate(seed, length));
    let mut h = Harness::new();
    let mut replay = repeat.then(Harness::new);
    let mut coverage = Coverage::default();
    for (step, command) in commands.iter().enumerate() {
        let before = old_kind(&h, command);
        let record = execute(&mut h, command, seed, step);
        coverage.record(command, before, &record);
        if let Some(replay) = &mut replay {
            let again = execute(replay, command, seed, step);
            assert_eq!(
                record, again,
                "{GENERATOR} seed={seed:02x?} step={step} 完整原序重放"
            );
        }
    }
    coverage.complete(length);
    eprintln!(
        "{GENERATOR} seed={seed:02x?} repeat={repeat}: {coverage:?}; history={} now={:?} trade_next={}",
        h.known_p.len(),
        h.clock.now(),
        h.trades.next
    );
    coverage
}

#[test]
fn cancel_replace_differential_focused_seed() {
    run(SEED_A, 1024, true);
}

#[test]
fn cancel_replace_differential_fixed_seed_100k_compares_every_command() {
    run(SEED_100K, 100_000, false);
}

#[test]
fn cancel_replace_differential_two_fixed_10k_seeds_repeat_complete_transcript() {
    for seed in [SEED_A, SEED_B] {
        run(seed, 10_000, true);
    }
}
