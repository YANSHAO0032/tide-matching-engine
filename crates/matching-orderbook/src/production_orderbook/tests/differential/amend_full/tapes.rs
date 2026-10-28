//! 固定种子的完整命令生成器；不读取订单簿状态，重复运行比较每条原序记录。
use super::*;

const GENERATOR: &str = "amend-full-v1-ChaCha";
const SEED_100K: [u8; 32] = [0xA2; 32];
const SEED_A: [u8; 32] = [0x3A; 32];
const SEED_B: [u8; 32] = [0xD7; 32];

fn kind(id: u128) -> u8 {
    ((id - 1) / 2) as u8
}
fn side(id: u128) -> Side {
    if id % 2 == 1 { Side::Buy } else { Side::Sell }
}

fn placement(id: u128, now: u64) -> Command {
    let side = side(id);
    let ordinary = order(id, side, if side == Side::Buy { 90 } else { 110 }, 12);
    match kind(id) {
        0 => Command::Rest(
            ordinary,
            if id % 2 == 1 {
                gtd(now + 8)
            } else {
                TimeInForce::Day {
                    session_id: SESSION,
                }
            },
        ),
        1 => Command::Iceberg(IcebergOrder::try_new(ordinary, Qty::try_new(3).unwrap()).unwrap()),
        k => Command::Stop(stop(
            id,
            k,
            side,
            if id % 2 == 1 {
                TriggerExpiry::Gtd {
                    expire_at: LogicalTime::new(now + 8),
                }
            } else {
                TriggerExpiry::Day {
                    session_id: SESSION,
                }
            },
        )),
    }
}

/// 每轮固定建簿、成功修改、签名重放、随机交错、触发/到期及清理共 128 条。
/// 输入域至多 24 个活动身份、余量至多 30，保证完整 child 级联所需成交标识有界。
fn generate(seed: [u8; 32], length: usize) -> Vec<Command> {
    assert!(length <= 100_000);
    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &seed);
    let mut runner = TestRunner::new_with_rng(Config::default(), rng);
    let strategy = (0u8..20, 1u128..=10, 2u64..=30, 85i64..=115, any::<bool>());
    let mut commands = Vec::with_capacity(length);
    let mut block = 0u64;
    while commands.len() < length {
        let now = 1 + block * 10;
        let mut batch = Vec::with_capacity(128);
        for id in 1..=10 {
            batch.push(placement(id, now));
        }
        for id in 1..=10 {
            let mut p = patch(id);
            if kind(id) == 3 {
                p.new_quote_budget = Some(QuoteAmount::new(900));
            } else {
                p.new_qty = Some(Qty::try_new(9).unwrap());
            }
            if kind(id) == 1 {
                p.new_visible_qty = Some(Qty::try_new(2).unwrap());
            }
            batch.push(amend(p));
        }
        let id = u128::from(block % 10 + 1);
        let mut p = patch(id);
        if kind(id) == 3 {
            p.new_quote_budget = Some(QuoteAmount::new(1500));
        } else {
            p.new_qty = Some(Qty::try_new(15).unwrap());
        }
        if kind(id) == 1 {
            p.new_visible_qty = Some(Qty::try_new(4).unwrap());
        }
        signed(&mut p, u128::from(block) + 100, 11);
        let authorization = Command::Amend {
            patch: p.into(),
            command_id: u128::from(block) + 100,
            required: 11_000,
        };
        batch.extend([authorization.clone(), authorization]);
        for source in [
            PriceReferenceKind::LastTrade,
            PriceReferenceKind::MarkPrice,
            PriceReferenceKind::IndexPrice,
        ] {
            batch.push(Command::Reference(PriceReference {
                kind: source,
                source_seq: (block + 1) * 10_000,
                price: Price::try_new(100).unwrap(),
            }));
        }
        while batch.len() < 100 {
            let offset = batch.len();
            let (variant, id, qty, price, buy) = strategy
                .new_tree(&mut runner)
                .unwrap_or_else(|error| {
                    panic!("{GENERATOR} seed={seed:02x?} block={block} offset={offset}: {error:?}")
                })
                .current();
            let mut p = patch(id);
            let command = match variant {
                0 => {
                    if kind(id) == 0 {
                        Command::ReservedPlace(order(
                            id,
                            side(id),
                            if side(id) == Side::Buy { 90 } else { 110 },
                            12,
                        ))
                    } else {
                        placement(id, now)
                    }
                }
                1 => Command::Cancel(OrderId::new(id)),
                2 => Command::Place(
                    order(
                        11 + id,
                        if buy { Side::Buy } else { Side::Sell },
                        price,
                        qty,
                    ),
                    gtd(now + 8),
                ),
                3 => {
                    if kind(id) == 3 {
                        p.new_quote_budget = Some(QuoteAmount::new(u128::from(qty) * 100));
                    } else {
                        p.new_qty = Some(Qty::try_new(qty).unwrap());
                    }
                    amend(p)
                }
                4 => {
                    p.new_price = Some(Price::try_new(price).unwrap());
                    amend(p)
                }
                5 => {
                    p.new_visible_qty = Some(Qty::try_new(1 + qty / 6).unwrap());
                    amend(p)
                }
                6 => {
                    p.new_stop_price = Some(Price::try_new(price).unwrap());
                    amend(p)
                }
                7 => {
                    p.new_trigger_reference = Some(if buy {
                        PriceReferenceKind::MarkPrice
                    } else {
                        PriceReferenceKind::IndexPrice
                    });
                    amend(p)
                }
                8 => {
                    p.new_trigger_expiry = Some(TriggerExpiryPatch::Set(TriggerExpiry::Gtd {
                        expire_at: LogicalTime::new(now + 5),
                    }));
                    amend(p)
                }
                9 => {
                    p.new_trigger_expiry = Some(if buy {
                        TriggerExpiryPatch::Keep
                    } else {
                        TriggerExpiryPatch::ClearToGtc
                    });
                    amend(p)
                }
                10 => {
                    p.new_tif = Some(if buy {
                        gtd(now + 5)
                    } else {
                        TimeInForce::Day {
                            session_id: SESSION,
                        }
                    });
                    amend(p)
                }
                11 => {
                    p.new_market_protection = Some(MarketProtectionPatch::Set(MarketProtection {
                        max_slippage_bps: Some(if buy { 75 } else { 10_000 }),
                        worst_price: Some(Price::try_new(price).unwrap()),
                    }));
                    amend(p)
                }
                12 => {
                    p.new_market_protection = Some(if buy {
                        MarketProtectionPatch::Clear
                    } else {
                        MarketProtectionPatch::Keep
                    });
                    amend(p)
                }
                13 => {
                    p.new_qty = Some(Qty::try_new(qty).unwrap());
                    p.new_quote_budget = Some(QuoteAmount::new(100));
                    amend(p)
                }
                14 => {
                    p.new_tif = Some(if buy {
                        gtd(now)
                    } else {
                        TimeInForce::Day { session_id: 8 }
                    });
                    amend(p)
                }
                15 => {
                    p.new_trigger_reference = Some(PriceReferenceKind::StaticOpening);
                    amend(p)
                }
                16 => {
                    p.user_id = UserId::new(100);
                    amend(p)
                }
                17 => Command::Amend {
                    patch: p.into(),
                    command_id: 80,
                    required: 13_000,
                },
                18 => Command::Reference(PriceReference {
                    kind: if buy {
                        PriceReferenceKind::MarkPrice
                    } else {
                        PriceReferenceKind::IndexPrice
                    },
                    source_seq: (block + 1) * 10_000 + offset as u64,
                    price: Price::try_new(price).unwrap(),
                }),
                19 => Command::EndSession(if buy { SESSION } else { 8 }),
                _ => unreachable!(),
            };
            batch.push(command);
        }
        // LastTrade 跨越由显式参考输入和真实 child 成交共同驱动，来源序号每轮有足够间隔。
        for price in [115, 85] {
            batch.push(Command::Reference(PriceReference {
                kind: PriceReferenceKind::LastTrade,
                source_seq: (block + 1) * 10_000 + if price == 115 { 1000 } else { 2000 },
                price: Price::try_new(price).unwrap(),
            }));
        }
        batch.push(Command::AdvanceTime(LogicalTime::new(now + 8)));
        batch.push(Command::EndSession(SESSION));
        for id in 1..=MAX_IDS {
            batch.push(Command::Cancel(OrderId::new(id)));
        }
        assert_eq!(batch.len(), 128);
        commands.extend(batch.into_iter().take(length - commands.len()));
        block += 1;
    }
    commands
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Coverage {
    /// 已执行的总命令数。
    commands: usize,
    /// 成功提交的改单次数。
    successful_amend: usize,
    /// 被拒绝且保持原子性的改单次数。
    rejected_amend: usize,
    /// 成功应用签名预留调整的次数。
    signed_success: usize,
    /// 成功修改止损订单的次数。
    stop_amend: usize,
    /// 成功修改冰山订单的次数。
    iceberg_amend: usize,
    /// 全部命令累计产生的成交数量。
    trades: usize,
    /// 全部命令累计触发的止损子订单数量。
    triggered: usize,
    /// 全部命令累计到期的订单数量。
    expired: usize,
}

fn run(commands: &[Command], seed: [u8; 32], mut record: impl FnMut(usize, Record)) -> Coverage {
    let mut h = Harness::new();
    let mut coverage = Coverage::default();
    for (step, command) in commands.iter().enumerate() {
        let before_stop = match command {
            Command::Amend { patch, .. } => h.p.stop_order(patch.order_id).is_some(),
            _ => false,
        };
        let before_iceberg = match command {
            Command::Amend { patch, .. } => {
                h.p.order(patch.order_id)
                    .is_some_and(|o| o.peak_size().is_some())
            }
            _ => false,
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| h.run(command)))
            .unwrap_or_else(|error| {
                eprintln!("{GENERATOR} seed={seed:02x?} step={step} command={command:?}");
                std::panic::resume_unwind(error)
            });
        coverage.commands += 1;
        if let Command::Amend { patch, .. } = command {
            if result.outcome.failed() {
                coverage.rejected_amend += 1;
            } else {
                coverage.successful_amend += 1;
                coverage.signed_success += usize::from(patch.reservation_adjustment.is_some());
                coverage.stop_amend += usize::from(before_stop);
                coverage.iceberg_amend += usize::from(before_iceberg);
            }
        }
        coverage.trades += result
            .outcome
            .events()
            .iter()
            .filter(|e| matches!(e, EngineEvent::Trade(_)))
            .count();
        coverage.triggered += result
            .outcome
            .events()
            .iter()
            .filter(|e| matches!(e, EngineEvent::StopTriggered { .. }))
            .count();
        if let Outcome::Expired(Ok(expired)) = &result.outcome {
            coverage.expired += expired.len();
        }
        record(step, result);
    }
    assert_eq!(coverage.commands, commands.len());
    assert!(coverage.successful_amend > 0 && coverage.rejected_amend > 0);
    assert!(coverage.signed_success > 0 && coverage.stop_amend > 0 && coverage.iceberg_amend > 0);
    assert!(coverage.trades > 0 && coverage.triggered > 0 && coverage.expired > 0);
    eprintln!("{GENERATOR} seed={seed:02x?}: {coverage:?}");
    coverage
}

#[test]
fn amend_full_tape_fixed_seed_100k_compares_every_command() {
    let commands = generate(SEED_100K, 100_000);
    run(&commands, SEED_100K, |_, _| {});
}

#[test]
fn amend_full_tape_two_fixed_10k_seeds_repeat_original_order_full_transcript() {
    for seed in [SEED_A, SEED_B] {
        let commands = generate(seed, 10_000);
        assert_eq!(commands, generate(seed, 10_000));
        let mut transcript = Vec::with_capacity(commands.len());
        let first = run(&commands, seed, |_, record| transcript.push(record));
        let mut expected = transcript.into_iter();
        let second = run(&commands, seed, |step, record| {
            assert_eq!(
                expected.next().unwrap(),
                record,
                "{GENERATOR} seed={seed:02x?} step={step}"
            );
        });
        assert_eq!(expected.next(), None);
        assert_eq!(first, second);
    }
}
