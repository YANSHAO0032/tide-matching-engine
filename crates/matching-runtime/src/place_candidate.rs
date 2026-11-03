//! 正式限价下单的分片内候选状态，不安装真实状态。
use crate::client_order_id::ClientOrderIds;
use matching_domain::{
    CommandId, EngineEvent, LogicalTime, OrderId, OriginSessionId, PlaceLimitOrder, Qty,
    RejectReason, Side, TimeInForce, TradeIdCursor,
};
use matching_orderbook::production_orderbook::{
    ProductionCancelReplaceContext, ProductionOrderBook, ProductionPlaceOrder,
};
use matching_risk::{
    ExpectedNewReservation, LimitReserveError, LimitReservePolicy, ReservationRegistry, RiskKeySet,
};
use std::collections::BTreeMap;

/// 同时携带所有需由单写者安装的状态与原序事件。
pub(crate) struct PlaceCandidate {
    /// 撮合后的市场簿。
    pub book: ProductionOrderBook,
    /// 接管与终结归属迁移后的登记表。
    pub registry: ReservationRegistry,
    /// 更新后的客户端订单身份索引。
    pub ids: ClientOrderIds,
    /// 实际成交推进后的根游标。
    pub cursor: TradeIdCursor,
    /// 接收、成交与生命周期事件。
    pub events: Vec<EngineEvent>,
}

/// 下单计划使用的可信策略、身份与逻辑时间。
pub(crate) struct PlacePlanningContext<'a> {
    /// 可信市场资金规则。
    pub policy: &'a LimitReservePolicy,
    /// 可信签名密钥集合。
    pub keys: &'a RiskKeySet,
    /// 根命令身份。
    pub command_id: CommandId,
    /// 首次执行的来源会话。
    pub session: u64,
    /// 分片业务逻辑时间。
    pub now: LogicalTime,
}

/// 对可信市场策略计算预留并在副本执行；真实活动订单容量由调用方检查。
/// 调用方还须先完成路由、用户控制检查并负责事件索引检查和原子安装。
/// 客户端身份候选须由调用方在分配序号前使用 `try_clone_preallocated` 创建。
pub(crate) fn place_candidate(
    context: PlacePlanningContext<'_>,
    place: &PlaceLimitOrder,
    book: &ProductionOrderBook,
    registry: &ReservationRegistry,
    mut candidate_ids: ClientOrderIds,
    cursor: &TradeIdCursor,
) -> Result<PlaceCandidate, RejectReason> {
    let PlacePlanningContext {
        policy,
        keys,
        command_id,
        session,
        now,
    } = context;
    let market = book.market_id().ok_or(RejectReason::MarketNotFound)?;
    if !book.stop_orders(Side::Buy).is_empty() || !book.stop_orders(Side::Sell).is_empty() {
        return Err(RejectReason::InvalidOrderCombination);
    }
    let math = policy.market_math();
    let order = &place.order;
    if order.price.get() % math.tick_size() != 0 {
        return Err(RejectReason::InvalidTick);
    }
    if !order.qty.get().is_multiple_of(math.lot_size()) {
        return Err(RejectReason::InvalidLot);
    }
    let required_reserves = policy
        .required_reserves(order.side, order.price, order.qty)
        .map_err(|e| match e {
            LimitReserveError::Math(error) => error
                .to_reject_reason()
                .expect("已验证市场配置不能产生非整数本金"),
            LimitReserveError::Overflow => RejectReason::ArithmeticOverflow,
            LimitReserveError::InvalidPolicy => RejectReason::InvalidReservation,
        })?;
    let expected = ExpectedNewReservation {
        command_id,
        order_id: order.order_id,
        user_id: order.user_id,
        market_id: market,
        origin_session_id: OriginSessionId::new(session),
        fee_schedule_version: policy.fee_schedule_version(),
        fee_tier_id: policy.fee_tier_id(),
        required_reserves,
    };
    let plan = registry.plan_new_reservation(keys, &expected, &place.reservation)?;
    let state = plan.active_state().clone();
    let mut candidate_registry = registry.clone();
    candidate_registry
        .apply_new_reservation(plan)
        .expect("候选预留前态不应漂移");
    if let Some(id) = &place.client_order_id {
        candidate_ids.accept_in_market(market, order.user_id, id.clone(), order.order_id)?;
    }
    let mut candidate_book = book.clone();
    let mut candidate_cursor = cursor.clone();
    let raw = candidate_book.place_with_registry_and_cursor(
        ProductionPlaceOrder::Limit {
            order: order.clone(),
            time_in_force: TimeInForce::Gtc,
            post_only: place.post_only,
        },
        state,
        &mut candidate_registry,
        ProductionCancelReplaceContext {
            math: &math,
            now,
            current_session_id: session,
            trade_ids: &[],
        },
        &mut candidate_cursor,
    )?;
    let mut events = vec![EngineEvent::OrderAccepted {
        order_id: order.order_id,
    }];
    let mut remaining = BTreeMap::new();
    let mut had_trade = false;
    let mut taker_terminal = false;
    for event in raw {
        if let EngineEvent::Trade(trade) = &event {
            had_trade = true;
            let maker = book
                .order(trade.maker_order_id)
                .expect("成交挂单必须存在于前态");
            let qty = remaining
                .entry(trade.maker_order_id)
                .or_insert(maker.total_remaining());
            *qty = qty
                .checked_sub(trade.qty.get())
                .expect("成交数量不能超过挂单余量");
            events.push(event.clone());
            if maker.peak_size().is_none() {
                events.push(maker_fill_event(trade.maker_order_id, *qty)?);
            }
        } else {
            if matches!(&event, EngineEvent::OrderCanceled { order_id, .. } | EngineEvent::OrderFilled { order_id } if *order_id == order.order_id)
            {
                taker_terminal = true;
            }
            events.push(event);
        }
    }
    if !taker_terminal {
        if let Some(node) = candidate_book.order(order.order_id) {
            if had_trade {
                events.push(maker_fill_event(order.order_id, node.total_remaining())?);
            }
            events.push(EngineEvent::OrderOpened {
                order_id: order.order_id,
            });
        } else if had_trade {
            events.push(EngineEvent::OrderFilled {
                order_id: order.order_id,
            });
        }
    }
    for event in &events {
        let terminal = match event {
            EngineEvent::OrderFilled { order_id } | EngineEvent::OrderCanceled { order_id, .. } => {
                Some(*order_id)
            }
            _ => None,
        };
        if let Some(id) = terminal
            && candidate_book.order(id).is_none()
        {
            candidate_ids.terminal_in_market(market, id);
        }
    }
    Ok(PlaceCandidate {
        book: candidate_book,
        registry: candidate_registry,
        ids: candidate_ids,
        cursor: candidate_cursor,
        events,
    })
}

/// 根据真实剩余数量构造一次成交后的生命周期事件。
fn maker_fill_event(order_id: OrderId, remaining: u64) -> Result<EngineEvent, RejectReason> {
    if remaining == 0 {
        Ok(EngineEvent::OrderFilled { order_id })
    } else {
        Ok(EngineEvent::OrderPartiallyFilled {
            order_id,
            remaining: Qty::try_new(remaining)?,
        })
    }
}
#[cfg(test)]
mod tests {
    //! 验证活动订单生命周期事件的补全。
    use super::*;
    #[test]
    fn maker_fill_event_is_derived_from_real_remaining() {
        assert_eq!(
            maker_fill_event(OrderId::new(1), 0),
            Ok(EngineEvent::OrderFilled {
                order_id: OrderId::new(1)
            })
        );
        assert_eq!(
            maker_fill_event(OrderId::new(1), 3),
            Ok(EngineEvent::OrderPartiallyFilled {
                order_id: OrderId::new(1),
                remaining: Qty::try_new(3).unwrap()
            })
        );
    }
}

#[cfg(test)]
mod integration_tests {
    //! 使用真实签名、订单簿和登记表验证候选状态。
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use matching_domain::{
        AssetId, AssetReserve, ClusterId, LimitOrder, MarketId, MarketMath, Price, ReservationId,
        ReservationToken, ShardId, StpKey, StpMode, UserId,
    };
    use matching_orderbook::QueuePriorityPolicy;

    /// 构造候选下单测试所需的市场、预留与身份前态。
    fn setup() -> (
        LimitReservePolicy,
        RiskKeySet,
        ProductionOrderBook,
        ReservationRegistry,
        ClientOrderIds,
        TradeIdCursor,
    ) {
        let signing = SigningKey::from_bytes(&[7; 32]);
        let keys = RiskKeySet::new(1, [(31, signing.verifying_key())]).unwrap();
        let policy = LimitReservePolicy::new(
            (AssetId(1), 0),
            (AssetId(2), 0),
            MarketMath::try_new(0, 0, 0, 1, 1).unwrap(),
            9,
            10,
            0,
        )
        .unwrap();
        let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(1024).unwrap());
        book.bind_market(MarketId(1)).unwrap();
        (
            policy,
            keys,
            book,
            ReservationRegistry::new([]).unwrap(),
            ClientOrderIds::new(20, 20).unwrap(),
            TradeIdCursor::new(ClusterId(1), ShardId(1), 1),
        )
    }
    /// 构造指定身份、方向和数量的测试限价单。
    fn order(id: u64, user: u64, side: Side, qty: u64, post_only: bool) -> PlaceLimitOrder {
        let command_id = CommandId(u128::from(id));
        let mut token = ReservationToken {
            token_id: u128::from(id),
            command_id,
            reservation_id: ReservationId(u128::from(id)),
            order_id: OrderId(u128::from(id)),
            user_id: UserId(user),
            market_id: MarketId(1),
            reserves: vec![AssetReserve {
                asset: if side == Side::Buy {
                    AssetId(2)
                } else {
                    AssetId(1)
                },
                atoms: if side == Side::Buy {
                    u128::from(qty) * 10
                } else {
                    u128::from(qty)
                },
            }]
            .into(),
            fee_schedule_version: 9,
            fee_tier_id: 10,
            risk_key_id: 31,
            signature: [0; 64],
        };
        token.signature = SigningKey::from_bytes(&[7; 32])
            .sign(&token.canonical_signing_payload().unwrap())
            .to_bytes();
        PlaceLimitOrder {
            order: LimitOrder {
                order_id: OrderId(u128::from(id)),
                user_id: UserId(user),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(qty).unwrap(),
            },
            client_order_id: Some(format!("c{id}")),
            user_control_epoch: 0,
            post_only,
            reservation: token,
        }
    }
    #[test]
    fn post_only_and_signature_failures_leave_sources_unchanged() {
        let (policy, keys, book, registry, ids, cursor) = setup();
        let first = order(1, 1, Side::Sell, 2, true);
        let result = place_candidate(
            PlacePlanningContext {
                policy: &policy,
                keys: &keys,
                command_id: CommandId(1),
                session: 77,
                now: LogicalTime::new(0),
            },
            &first,
            &book,
            &registry,
            ids.try_clone_preallocated().unwrap(),
            &cursor,
        )
        .unwrap();
        assert_eq!(
            result.events,
            vec![
                EngineEvent::OrderAccepted {
                    order_id: OrderId(1)
                },
                EngineEvent::OrderOpened {
                    order_id: OrderId(1)
                }
            ]
        );
        assert!(book.order(OrderId(1)).is_none());
        assert_eq!(
            result
                .registry
                .get(ReservationId(1))
                .unwrap()
                .origin_session_id,
            OriginSessionId(77)
        );
        let snapshot = (
            result.book.clone(),
            result.registry.clone(),
            result.ids.clone(),
            result.cursor.clone(),
        );
        let crossing = order(2, 2, Side::Buy, 1, true);
        assert!(matches!(
            place_candidate(
                PlacePlanningContext {
                    policy: &policy,
                    keys: &keys,
                    command_id: CommandId(2),
                    session: 77,
                    now: LogicalTime::new(0)
                },
                &crossing,
                &result.book,
                &result.registry,
                result.ids.try_clone_preallocated().unwrap(),
                &result.cursor
            ),
            Err(RejectReason::PostOnlyWouldCross)
        ));
        let mut bad = crossing;
        bad.reservation.signature[0] ^= 1;
        assert!(matches!(
            place_candidate(
                PlacePlanningContext {
                    policy: &policy,
                    keys: &keys,
                    command_id: CommandId(2),
                    session: 77,
                    now: LogicalTime::new(0)
                },
                &bad,
                &result.book,
                &result.registry,
                result.ids.try_clone_preallocated().unwrap(),
                &result.cursor
            ),
            Err(RejectReason::InvalidReservation)
        ));
        assert_eq!(
            (result.book, result.registry, result.ids, result.cursor),
            snapshot
        );
    }
    #[test]
    fn multiple_makers_emit_ordered_fills_and_terminal_ids() {
        let (p, k, b, r, i, c) = setup();
        let a = place_candidate(
            PlacePlanningContext {
                policy: &p,
                keys: &k,
                command_id: CommandId(1),
                session: 1,
                now: LogicalTime::new(0),
            },
            &order(1, 1, Side::Sell, 2, false),
            &b,
            &r,
            i.try_clone_preallocated().unwrap(),
            &c,
        )
        .unwrap();
        let b = place_candidate(
            PlacePlanningContext {
                policy: &p,
                keys: &k,
                command_id: CommandId(2),
                session: 1,
                now: LogicalTime::new(0),
            },
            &order(2, 2, Side::Sell, 2, false),
            &a.book,
            &a.registry,
            a.ids.try_clone_preallocated().unwrap(),
            &a.cursor,
        )
        .unwrap();
        let x = place_candidate(
            PlacePlanningContext {
                policy: &p,
                keys: &k,
                command_id: CommandId(3),
                session: 1,
                now: LogicalTime::new(0),
            },
            &order(3, 3, Side::Buy, 3, false),
            &b.book,
            &b.registry,
            b.ids.try_clone_preallocated().unwrap(),
            &b.cursor,
        )
        .unwrap();
        assert!(matches!(
            x.events.as_slice(),
            [
                EngineEvent::OrderAccepted { .. },
                EngineEvent::Trade(_),
                EngineEvent::OrderFilled {
                    order_id: OrderId(1)
                },
                EngineEvent::Trade(_),
                EngineEvent::OrderPartiallyFilled {
                    order_id: OrderId(2),
                    ..
                },
                EngineEvent::OrderFilled {
                    order_id: OrderId(3)
                }
            ]
        ));
        assert!(x.book.order(OrderId(1)).is_none());
        assert_eq!(
            x.ids.lookup(UserId(1), "c1"),
            Some((MarketId(1), OrderId(1)))
        );
    }
    #[test]
    fn stp_cancel_maker_precedes_later_trade() {
        let (p, k, b, r, i, c) = setup();
        let a = place_candidate(
            PlacePlanningContext {
                policy: &p,
                keys: &k,
                command_id: CommandId(1),
                session: 1,
                now: LogicalTime::new(0),
            },
            &order(1, 9, Side::Sell, 2, false),
            &b,
            &r,
            i.try_clone_preallocated().unwrap(),
            &c,
        )
        .unwrap();
        let b = place_candidate(
            PlacePlanningContext {
                policy: &p,
                keys: &k,
                command_id: CommandId(2),
                session: 1,
                now: LogicalTime::new(0),
            },
            &order(2, 2, Side::Sell, 2, false),
            &a.book,
            &a.registry,
            a.ids.try_clone_preallocated().unwrap(),
            &a.cursor,
        )
        .unwrap();
        let mut taker = order(3, 9, Side::Buy, 2, false);
        taker.order.stp_mode = StpMode::CancelMaker;
        let x = place_candidate(
            PlacePlanningContext {
                policy: &p,
                keys: &k,
                command_id: CommandId(3),
                session: 1,
                now: LogicalTime::new(0),
            },
            &taker,
            &b.book,
            &b.registry,
            b.ids.try_clone_preallocated().unwrap(),
            &b.cursor,
        )
        .unwrap();
        assert!(matches!(
            x.events.as_slice(),
            [
                EngineEvent::OrderAccepted { .. },
                EngineEvent::OrderCanceled {
                    order_id: OrderId(1),
                    ..
                },
                EngineEvent::Trade(_),
                EngineEvent::OrderFilled {
                    order_id: OrderId(2)
                },
                EngineEvent::OrderFilled {
                    order_id: OrderId(3)
                }
            ]
        ));
    }
    #[test]
    fn iceberg_maker_events_keep_total_remaining_without_duplicates() {
        use matching_domain::IcebergOrder;
        let (p, k, mut b, mut r, mut ids, mut c) = setup();
        let maker = order(1, 1, Side::Sell, 5, false);
        let expected = ExpectedNewReservation {
            command_id: CommandId(1),
            order_id: OrderId(1),
            user_id: UserId(1),
            market_id: MarketId(1),
            origin_session_id: OriginSessionId(77),
            fee_schedule_version: 9,
            fee_tier_id: 10,
            required_reserves: p
                .required_reserves(Side::Sell, maker.order.price, maker.order.qty)
                .unwrap(),
        };
        let plan = r
            .plan_new_reservation(&k, &expected, &maker.reservation)
            .unwrap();
        let state = plan.active_state().clone();
        r.apply_new_reservation(plan).unwrap();
        ids.accept_in_market(MarketId(1), UserId(1), "c1".into(), OrderId(1))
            .unwrap();
        let math = p.market_math();
        b.place_with_registry_and_cursor(
            ProductionPlaceOrder::Iceberg {
                order: IcebergOrder::try_new(maker.order, Qty::try_new(2).unwrap()).unwrap(),
                post_only: true,
            },
            state,
            &mut r,
            ProductionCancelReplaceContext {
                math: &math,
                now: LogicalTime::new(0),
                current_session_id: 77,
                trade_ids: &[],
            },
            &mut c,
        )
        .unwrap();
        let taker = order(2, 2, Side::Buy, 3, false);
        let x = place_candidate(
            PlacePlanningContext {
                policy: &p,
                keys: &k,
                command_id: CommandId(2),
                session: 88,
                now: LogicalTime::new(0),
            },
            &taker,
            &b,
            &r,
            ids.try_clone_preallocated().unwrap(),
            &c,
        )
        .unwrap();
        assert!(
            matches!(x.events.as_slice(), [EngineEvent::OrderAccepted { .. }, EngineEvent::Trade(_), EngineEvent::OrderPartiallyFilled { order_id: OrderId(1), remaining: a }, EngineEvent::IcebergRefreshed { order_id: OrderId(1), .. }, EngineEvent::Trade(_), EngineEvent::OrderPartiallyFilled { order_id: OrderId(1), remaining: z }, EngineEvent::OrderFilled { order_id: OrderId(2) }] if a.get() == 3 && z.get() == 2)
        );
        assert_eq!(
            x.registry.get(ReservationId(1)).unwrap().origin_session_id,
            OriginSessionId(77)
        );
        assert_eq!(
            x.registry.get(ReservationId(2)).unwrap().origin_session_id,
            OriginSessionId(88)
        );
    }
    #[test]
    fn nonempty_stop_book_rejects_before_mutation() {
        use matching_domain::{PriceReferenceKind, StopLimitOrder, StopOrder, TriggerExpiry};
        let (p, k, mut b, r, ids, c) = setup();
        b.place_stop(
            StopOrder::Limit(StopLimitOrder {
                trigger_reference: PriceReferenceKind::LastTrade,
                order_id: OrderId(9),
                user_id: UserId(9),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                reservation_id: ReservationId(9),
                fee_schedule_version: 9,
                side: Side::Buy,
                stop_price: Price::try_new(20).unwrap(),
                price: Price::try_new(20).unwrap(),
                qty: Qty::try_new(1).unwrap(),
                child_tif: TimeInForce::Gtc,
                trigger_expiry: TriggerExpiry::Gtc,
            }),
            LogicalTime::new(0),
            1,
        )
        .unwrap();
        let before = b.clone();
        assert!(matches!(
            place_candidate(
                PlacePlanningContext {
                    policy: &p,
                    keys: &k,
                    command_id: CommandId(1),
                    session: 1,
                    now: LogicalTime::new(0)
                },
                &order(1, 1, Side::Sell, 1, false),
                &b,
                &r,
                ids.try_clone_preallocated().unwrap(),
                &c
            ),
            Err(RejectReason::InvalidOrderCombination)
        ));
        assert_eq!(b, before);
    }
}
