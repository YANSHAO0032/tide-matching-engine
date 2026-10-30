//! 受控命令序列的输入域、固定种子生成器、独立执行适配与逐命令回放。
use super::*;

/// 基础命令序列的测试域约束：在创建订单簿之前检查完整命令序列。
///
/// 参考簿适配器采用成交 -> 余量挂单，尚未实现一般情况下的
/// 失败原子事务；因此极值输入不能进入逐命令执行阶段。
/// DuplicateOrderId / OrderNotFound 不属于输入域错误。
pub(super) const MAX_TAPE_LENGTH: usize = 100_000;

pub(super) fn validate_tape_domain(commands: &[Command]) -> Result<(), String> {
    if commands.len() > MAX_TAPE_LENGTH {
        return Err(format!(
            "tape length={} outside allowed range 0..=100000",
            commands.len()
        ));
    }

    for (index, command) in commands.iter().enumerate() {
        // 诊断优先级：OrderId 必须先于下单价格或数量。
        let id = match command {
            Command::Place(order) => order.order_id.get(),
            Command::Cancel(order_id) => order_id.get(),
            Command::MarketByBase(order, _) => order.order_id.get(),
            Command::MarketByQuote(order, _) => order.order_id.get(),
        };

        if !(1..=128).contains(&id) {
            return Err(format!(
                "command[{index}] OrderId={id} outside allowed range 1..=128"
            ));
        }

        if let Command::Place(order) = command {
            let price = order.price.get();
            if !(80..=120).contains(&price) {
                return Err(format!(
                    "command[{index}] Place price={price} outside allowed range 80..=120"
                ));
            }

            let qty = order.qty.get();
            if !(1..=100).contains(&qty) {
                return Err(format!(
                    "command[{index}] Place qty={qty} outside allowed range 1..=100"
                ));
            }
        }

        if let Command::MarketByBase(order, _) = command {
            let qty = order.qty.get();
            if !(1..=100).contains(&qty) {
                return Err(format!(
                    "command[{index}] MarketByBase qty={qty} outside allowed range 1..=100"
                ));
            }
        }

        if let Command::MarketByQuote(order, _) = command {
            let budget = order.quote_budget.get();

            // 固定短命令序列的受控预算范围。
            // 不修改随机生成器的抽样范围。
            if !(1..=20_000).contains(&budget) {
                return Err(format!(
                    "command[{index}] MarketByQuote budget={budget} \
             outside allowed range 1..=20000"
                ));
            }
        }
    }

    Ok(())
}

/// 固定种子随机命令生成器；测试输入独立于订单簿状态。
pub(super) const TAPE_GENERATOR_VERSION: &str = "T05.2b-v1";

pub(super) fn generate_tape(seed: [u8; 32], length: usize) -> Vec<Command> {
    assert!(
        length <= MAX_TAPE_LENGTH,
        "generate_tape length={length} outside allowed range 0..=100000"
    );

    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &seed);
    let mut runner = TestRunner::new_with_rng(Config::default(), rng);
    // 种类 0..=2: 下单; 种类 3: 取消; 买入/卖出概率各半。
    let strategy = (0u8..4, 1u128..=128, 80i64..=120, 1u64..=100, any::<bool>());
    let mut commands = Vec::with_capacity(length);

    for index in 0..length {
        let tree = strategy.new_tree(&mut runner).unwrap_or_else(|error| {
            panic!(
                "tape sampling failed: seed={seed:02x?}, algorithm=ChaCha, \
                 index={index}, error={error:?}"
            )
        });
        let (kind, id, price, qty, is_buy) = tree.current();
        let command = if kind < 3 {
            let side = if is_buy { Side::Buy } else { Side::Sell };
            Command::Place(order(id, side, price, qty))
        } else {
            Command::Cancel(OrderId::new(id))
        };
        commands.push(command);
    }

    validate_tape_domain(&commands)
        .unwrap_or_else(|reason| panic!("generated invalid tape: {reason}"));
    commands
}

/// 参考簿完整限价单 GTC 差分适配。
///
/// 生产簿:
///
///     place_limit_gtc
///         -> 预检
///         -> 成交
///         -> 余量挂单
///
/// 参考簿当前是低层组合：
///
///     重复标识符检查
///         -> execute_next_fill_trade*
///         -> 实际余量
///         -> 调用 `rest` 挂入实际余量
///
/// 此适配使用小数量、远离优先级上限的受控输入，因此这里只允许：
///
/// - DuplicateOrderId 作为受控下单错误；
/// - 正常成交 / 挂单。
///
/// ArithmeticOverflow / PrioritySpaceExhaustion 等其它错误一律立即触发不变量断言，
/// 不把这个测试适配器描述成通用失败原子事务。
pub(super) fn place_reference(
    book: &mut ReferenceOrderBook,
    order: LimitOrder,
    trade_ids: &[TradeId],
) -> Result<ReferencePlaceResult, RejectReason> {
    // ------------------------------------------------------------
    // 1. 活动重复标识符必须先于任何状态变更。
    // ------------------------------------------------------------

    if reference_contains_order(book, order.order_id) {
        return Err(RejectReason::DuplicateOrderId);
    }

    // ------------------------------------------------------------
    // 2. 首笔状态变更前保证 TradeId 足量。
    //
    // 此普通订单测试域内，每张挂单对一张新到订单最多贡献一笔成交。
    // 对手侧当前有效订单总数因此是安全上界。
    //
    // 可以多给，不能少给。
    // ------------------------------------------------------------

    let maximum_fill_count = book.orders(opposite_side(order.side)).len();

    assert!(
        trade_ids.len() >= maximum_fill_count,
        "differential Reference adapter received insufficient TradeIds: \
         maximum_fill_count={maximum_fill_count}, provided={}",
        trade_ids.len()
    );

    let mut incoming = IncomingOrder::new(order.clone());
    let mut trades = Vec::new();

    // ------------------------------------------------------------
    // 3. 逐笔成交。
    // ------------------------------------------------------------

    for trade_id in trade_ids.iter().copied() {
        match book.execute_next_fill_trade(&mut incoming, trade_id) {
            Ok(Some(trade)) => {
                trades.push(trade);
            }

            Ok(None) => {
                break;
            }

            Err(reason) => {
                panic!(
                    "unexpected Reference fill error in T05.1 fixed differential tape: \
                     order_id={:?}, reason={reason:?}",
                    order.order_id
                );
            }
        }
    }

    // ------------------------------------------------------------
    // 4. 参考簿不自动挂单余量。
    //
    // 必须使用“实际剩余量”，不能把原始数量直接重新挂入。
    // ------------------------------------------------------------

    let rested = incoming.remaining() > 0;

    if rested {
        let residual_qty = Qty::try_new(incoming.remaining())
            .expect("positive Reference differential remainder must form Qty");

        let mut residual_order = order;

        residual_order.qty = residual_qty;

        book.rest(residual_order).unwrap_or_else(|reason| {
            panic!(
                "unexpected Reference residual-rest error in T05.1 fixed differential tape: \
                 reason={reason:?}"
            )
        });
    }

    Ok(ReferencePlaceResult { trades, rested })
}

/// 收集模式下执行一条受控命令序列。
///
/// 每条命令后都完成：
///
/// 1. 参考簿 / 生产簿命令结果对照；
/// 2. TradeEvent 原顺序对照；
/// 3. 双侧完整挂单订单对照；
/// 4. 剩余量 / 优先级对照；
/// 5. 深度对照；
/// 6. 最优价格对照；
/// 7. 生产簿 validate_book；
/// 8. 生产簿 `next` 优先级 == 成功挂单次数。
pub(super) fn run_tape(commands: &[Command]) -> Vec<StepRecord> {
    let mut transcript = Vec::with_capacity(commands.len());
    execute_tape(commands, |record| transcript.push(record));
    transcript
}

/// 不存储完整执行记录；每步验证完成后仅增加计数。
pub(super) fn run_tape_streaming(commands: &[Command]) -> usize {
    let mut processed = 0usize;
    execute_tape(commands, |_record| {
        processed += 1;
    });
    processed
}

/// 唯一的差分执行核心。
///
/// 状态顺序固定为：完整输入域预检、空簿初始化、命令前观察与失败快照、
/// 两套实现独立执行、结果和原序事件比较、命令后观察与不变量比较，最后才
/// 向回调交付 `StepRecord`。因此流式模式只丢弃已经验证的记录，不能退化为
/// 最终状态比较；任何失败都保留对应的步骤和命令上下文。
fn execute_tape(commands: &[Command], mut on_record: impl FnMut(StepRecord)) {
    validate_tape_domain(commands)
        .unwrap_or_else(|reason| panic!("invalid differential tape domain: {reason}"));

    let mut reference = ReferenceOrderBook::new();
    let mut production = ProductionOrderBook::new(policy());
    let quote_math = MarketMath::try_new(0, 0, 0, 1, 1)
        .expect("fixed MarketByQuote math configuration must be valid");

    let mut trade_ids = TradeIdSource::new();

    // 此基础适配器不直接读取参考簿的下一优先级。
    //
    // 因此这里独立记录“成功挂单次数”。
    // 生产簿分配器从 0 开始，每次成功挂单恰好推进一次。
    let mut expected_production_next_priority = 0u128;
    // 测试专用：全命令序列成交标识符唯一性，不随单步快照丢弃。
    let mut seen_trade_ids = BTreeSet::new();

    assert_eq!(validate_book(&production), Ok(()));

    for (step_index, command) in commands.iter().enumerate() {
        // ------------------------------------------------------------
        // 命令前，两套簿必须已经一致。
        // ------------------------------------------------------------

        let reference_before = observe_reference(&reference);
        let production_before = observe_production(&production);

        assert_eq!(
            reference_before, production_before,
            "books already diverged before differential command: \
             step={step_index}, command={command:?}"
        );

        // 生产簿完整失败原子性快照。
        let production_bids_before = level_snapshot(&production.bids);
        let production_asks_before = level_snapshot(&production.asks);
        let production_orders_before = production.orders.clone();
        let production_arena_before = production.arena.test_snapshot();
        let production_next_before = production.next_queue_priority();
        let production_policy_before = *production.queue_priority_allocator.policy();

        let outcome = match command {
            // ========================================================
            // 下单
            // ========================================================
            Command::Place(candidate) => {
                // 重复标识符必须先决定。
                //
                // 重复标识符命令不需要为撮合准备 TradeId，
                // 也不应该推进测试 TradeId 来源。
                let duplicate = reference_contains_order(&reference, candidate.order_id);

                let supplied_trade_ids = if duplicate {
                    Vec::new()
                } else {
                    // 此普通订单测试域内，每个对手侧有效订单至多需要一个 TradeId。
                    //
                    // 这是状态变更前确定的安全上界。
                    let maximum_fill_count = reference.orders(opposite_side(candidate.side)).len();

                    trade_ids.take(maximum_fill_count)
                };

                let reference_result =
                    place_reference(&mut reference, candidate.clone(), &supplied_trade_ids);

                let production_result =
                    production.place_limit_gtc(candidate.clone(), &supplied_trade_ids);

                match (reference_result, production_result) {
                    // ------------------------------------------------
                    // 两边成功。
                    // ------------------------------------------------
                    (Ok(reference_place), Ok(production_trades)) => {
                        // TradeEvent 必须保持实际成交顺序。
                        assert_eq!(
                            reference_place.trades,
                            production_trades.to_vec(),
                            "TradeEvent tape diverged: \
                             step={step_index}, command={command:?}"
                        );

                        // 两边是否最终挂入新到订单余量必须一致。
                        let production_rested = production.order(candidate.order_id).is_some();

                        let reference_rested =
                            reference_contains_order(&reference, candidate.order_id);

                        assert_eq!(
                            reference_place.rested, reference_rested,
                            "Reference adapter rest decision does not match \
                             Reference live book: \
                             step={step_index}, command={command:?}"
                        );

                        assert_eq!(
                            reference_place.rested, production_rested,
                            "Reference / Production residual-rest decision \
                             diverged: \
                             step={step_index}, command={command:?}"
                        );

                        // 只有最终存在实际余量并成功挂单，
                        // 才允许消耗优先级。
                        //
                        // 全部成交不推进。
                        if reference_place.rested {
                            expected_production_next_priority = expected_production_next_priority
                                .checked_add(1)
                                .expect("differential expected priority overflow");
                        }

                        CommandOutcome::Place(Ok(production_trades.to_vec()))
                    }

                    // ------------------------------------------------
                    // 此受控 GTC 输入域唯一允许的下单错误：
                    // 活动重复标识符。
                    // ------------------------------------------------
                    (Err(RejectReason::DuplicateOrderId), Err(RejectReason::DuplicateOrderId)) => {
                        assert_eq!(
                            observe_reference(&reference),
                            reference_before,
                            "DuplicateOrderId mutated Reference semantic state: \
                             step={step_index}"
                        );

                        assert_eq!(
                            observe_production(&production),
                            production_before,
                            "DuplicateOrderId mutated Production semantic state: \
                             step={step_index}"
                        );

                        assert_production_failure_state_unchanged(
                            &production,
                            &production_bids_before,
                            &production_asks_before,
                            &production_orders_before,
                            &production_arena_before,
                            production_next_before,
                            production_policy_before,
                        );

                        CommandOutcome::Place(Err(RejectReason::DuplicateOrderId))
                    }

                    // ------------------------------------------------
                    // 本步不接受其它错误。
                    // ------------------------------------------------
                    (reference_result, production_result) => {
                        panic!(
                            "unexpected Place differential result: \
                             step={step_index}, \
                             command={command:?}, \
                             reference={reference_result:?}, \
                             production={production_result:?}"
                        );
                    }
                }
            }

            // ========================================================
            // MarketByBase: 两套公开入口各自独立执行。
            // TradeId 由固定命令序列明确提供，不使用参考簿推导结果。
            // ========================================================
            Command::MarketByBase(candidate, supplied_trade_ids) => {
                let reference_result = reference.execute_market_by_base(
                    candidate.clone(),
                    supplied_trade_ids,
                    &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
                );
                let production_result = production.execute_market_by_base(
                    candidate.clone(),
                    supplied_trade_ids,
                    &matching_domain::MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
                );

                match (reference_result, production_result) {
                    (Ok(reference_trades), Ok(production_trades)) => {
                        // Vec 原序比较，不允许按标识符、价格或 TradeId 排序。
                        assert_eq!(
                            reference_trades, production_trades,
                            "MarketByBase TradeEvent order diverged: \
                             step={step_index}, command={command:?}"
                        );
                        assert!(
                            !reference_contains_order(&reference, candidate.order_id),
                            "MarketByBase taker unexpectedly rested in Reference: step={step_index}"
                        );
                        assert!(
                            production.order(candidate.order_id).is_none(),
                            "MarketByBase taker unexpectedly rested in Production: step={step_index}"
                        );
                        assert_eq!(
                            production.next_queue_priority(),
                            production_next_before,
                            "MarketByBase consumed QueuePriority: step={step_index}"
                        );
                        CommandOutcome::MarketByBase(Ok(production_trades.to_vec()))
                    }
                    (Err(RejectReason::DuplicateOrderId), Err(RejectReason::DuplicateOrderId)) => {
                        assert_eq!(observe_reference(&reference), reference_before);
                        assert_eq!(observe_production(&production), production_before);
                        assert_production_failure_state_unchanged(
                            &production,
                            &production_bids_before,
                            &production_asks_before,
                            &production_orders_before,
                            &production_arena_before,
                            production_next_before,
                            production_policy_before,
                        );
                        CommandOutcome::MarketByBase(Err(RejectReason::DuplicateOrderId))
                    }
                    (reference_result, production_result) => {
                        panic!(
                            "unexpected MarketByBase differential outcome: \
                             step={step_index}, command={command:?}, \
                             reference={reference_result:?}, production={production_result:?}"
                        );
                    }
                }
            }

            Command::MarketByQuote(candidate, supplied_trade_ids) => {
                // 两套模型独立调用各自公开入口。
                // 不能以参考簿的成交结果驱动生产簿。
                let reference_result = reference.execute_market_by_quote(
                    candidate.clone(),
                    &quote_math,
                    supplied_trade_ids,
                );

                let production_result = production.execute_market_by_quote(
                    candidate.clone(),
                    &quote_math,
                    supplied_trade_ids,
                );

                match (reference_result, production_result) {
                    (Ok(reference_execution), Ok(production_execution)) => {
                        // 比较完整结果值对象：
                        // - 原序 Vec<TradeEvent>
                        // - 已用预算 `spent_quote`
                        // - 未用预算 `unused_quote`
                        // 不排序、不投影、不重算本金。
                        assert_eq!(
                            reference_execution, production_execution,
                            "MarketByQuote execution diverged: \
                 step={step_index}, command={command:?}"
                        );

                        // 成功市价吃单方不能成为活动挂单订单。
                        assert!(
                            !reference_contains_order(&reference, candidate.order_id),
                            "MarketByQuote taker rested in Reference: \
                 step={step_index}"
                        );

                        assert!(
                            production.order(candidate.order_id).is_none(),
                            "MarketByQuote taker rested in Production: \
                 step={step_index}"
                        );

                        // 报价资产市价单不消费 QueuePriority。
                        assert_eq!(
                            production.next_queue_priority(),
                            production_next_before,
                            "MarketByQuote consumed QueuePriority: \
                 step={step_index}"
                        );

                        CommandOutcome::MarketByQuote(Ok(production_execution))
                    }

                    (Err(RejectReason::DuplicateOrderId), Err(RejectReason::DuplicateOrderId)) => {
                        // 重复标识符必须在挂单状态变更前拒绝。
                        assert_eq!(
                            observe_reference(&reference),
                            reference_before,
                            "MarketByQuote duplicate mutated Reference: \
                 step={step_index}"
                        );

                        assert_eq!(
                            observe_production(&production),
                            production_before,
                            "MarketByQuote duplicate mutated Production: \
                 step={step_index}"
                        );

                        assert_production_failure_state_unchanged(
                            &production,
                            &production_bids_before,
                            &production_asks_before,
                            &production_orders_before,
                            &production_arena_before,
                            production_next_before,
                            production_policy_before,
                        );

                        CommandOutcome::MarketByQuote(Err(RejectReason::DuplicateOrderId))
                    }

                    (reference_result, production_result) => {
                        panic!(
                            "unexpected MarketByQuote differential outcome: \
                 step={step_index}, command={command:?}, \
                 reference={reference_result:?}, \
                 production={production_result:?}"
                        );
                    }
                }
            }

            // ========================================================
            // 取消
            // ========================================================
            Command::Cancel(order_id) => {
                let reference_result = reference.cancel(*order_id);
                let production_result = production.cancel(*order_id);

                match (reference_result, production_result) {
                    // ------------------------------------------------
                    // 两边成功。
                    // ------------------------------------------------
                    (Ok(reference_removed), Ok(production_removed)) => {
                        let reference_removed = observe_reference_order(&reference_removed);

                        let production_removed = observe_production_order(&production_removed);

                        assert_eq!(
                            reference_removed, production_removed,
                            "Cancel removed payload diverged: \
                             step={step_index}, order_id={order_id:?}"
                        );

                        // 取消不消费 QueuePriority。
                        assert_eq!(
                            production.next_queue_priority(),
                            production_next_before,
                            "successful Cancel consumed Production \
                             QueuePriority: \
                             step={step_index}, order_id={order_id:?}"
                        );

                        CommandOutcome::Cancel(Ok(production_removed))
                    }

                    // ------------------------------------------------
                    // 此受控输入域唯一允许的取消错误。
                    // ------------------------------------------------
                    (Err(RejectReason::OrderNotFound), Err(RejectReason::OrderNotFound)) => {
                        assert_eq!(
                            observe_reference(&reference),
                            reference_before,
                            "OrderNotFound mutated Reference semantic state: \
                             step={step_index}"
                        );

                        assert_eq!(
                            observe_production(&production),
                            production_before,
                            "OrderNotFound mutated Production semantic state: \
                             step={step_index}"
                        );

                        assert_production_failure_state_unchanged(
                            &production,
                            &production_bids_before,
                            &production_asks_before,
                            &production_orders_before,
                            &production_arena_before,
                            production_next_before,
                            production_policy_before,
                        );

                        CommandOutcome::Cancel(Err(RejectReason::OrderNotFound))
                    }

                    // ------------------------------------------------
                    // 本步不接受其它错误。
                    // ------------------------------------------------
                    (reference_result, production_result) => {
                        panic!(
                            "unexpected Cancel differential result: \
                             step={step_index}, \
                             order_id={order_id:?}, \
                             reference={reference_result:?}, \
                             production={production_result:?}"
                        );
                    }
                }
            }
        };

        // ------------------------------------------------------------
        // 每条命令后生产簿都必须通过整簿校验器。
        // ------------------------------------------------------------

        assert_eq!(
            validate_book(&production),
            Ok(()),
            "Production validator failed after differential command: \
             step={step_index}, command={command:?}"
        );

        // ------------------------------------------------------------
        // 每条命令后重新独立观察两套簿。
        // ------------------------------------------------------------

        let reference_after = observe_reference(&reference);
        let production_after = observe_production(&production);

        assert_eq!(
            reference_after, production_after,
            "Reference / Production semantic state diverged after command: \
             step={step_index}, command={command:?}"
        );

        // ------------------------------------------------------------
        // 此基础适配器沿用逐笔订单优先级比较，不直接读取参考簿计数器。
        //
        // 生产簿 `next` 必须等于成功挂单次数。
        // ------------------------------------------------------------

        assert_eq!(
            production.next_queue_priority(),
            expected_production_next_priority,
            "Production next QueuePriority diverged from successful-rest \
             count: \
             step={step_index}, command={command:?}"
        );

        // 所有原有比较、校验器、失败原子性及优先级检查通过之后，
        // 才核查当前普通订单簿不变量及本命令序列的成交标识符。
        assert_regular_book_invariants(
            &production_after,
            production.next_queue_priority(),
            step_index,
        );
        let successful_trades: Option<&[TradeEvent]> = match &outcome {
            CommandOutcome::Place(Ok(trades)) | CommandOutcome::MarketByBase(Ok(trades)) => {
                Some(trades)
            }

            CommandOutcome::MarketByQuote(Ok(execution)) => Some(execution.trades()),

            _ => None,
        };
        if let Some(trades) = successful_trades {
            for trade in trades {
                assert!(
                    trade.qty.get() > 0,
                    "zero TradeEvent qty: step={step_index}"
                );
                let id = trade.trade_id.get();
                assert!(
                    seen_trade_ids.insert(id),
                    "duplicate TradeId={id} in tape: step={step_index}"
                );
            }
        }

        on_record(StepRecord {
            outcome,
            book: production_after,
            production_next_priority: production.next_queue_priority(),
        });
    }
}
