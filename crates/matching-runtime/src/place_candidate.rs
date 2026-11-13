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
    candidate_ids: ClientOrderIds,
    cursor: &TradeIdCursor,
) -> Result<PlaceCandidate, RejectReason> {
    place_candidate_inner(context, place, book, registry, candidate_ids, cursor, None)
}

/// 由可信版本费率或旧策略构造独立下单候选。
fn place_candidate_inner(
    context: PlacePlanningContext<'_>,
    place: &PlaceLimitOrder,
    book: &ProductionOrderBook,
    registry: &ReservationRegistry,
    mut candidate_ids: ClientOrderIds,
    cursor: &TradeIdCursor,
    fees: Option<(&matching_domain::FeeScheduleBook, u64)>,
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
    let (fee_schedule_version, fee_tier_id, required_reserves) = if let Some((fees, version)) = fees
    {
        if place.reservation.fee_schedule_version != version {
            return Err(RejectReason::FeeScheduleMismatch);
        }
        let tier = place.reservation.fee_tier_id;
        let required = policy.required_versioned_reserves(
            fees,
            version,
            tier,
            order.side,
            order.price,
            order.qty,
        )?;
        (version, tier, required)
    } else {
        let required = policy
            .required_reserves(order.side, order.price, order.qty)
            .map_err(|e| match e {
                LimitReserveError::Math(error) => error
                    .to_reject_reason()
                    .expect("已验证市场配置不能产生非整数本金"),
                LimitReserveError::Overflow => RejectReason::ArithmeticOverflow,
                LimitReserveError::InvalidPolicy => RejectReason::InvalidReservation,
            })?;
        (
            policy.fee_schedule_version(),
            policy.fee_tier_id(),
            required,
        )
    };
    let expected = ExpectedNewReservation {
        command_id,
        order_id: order.order_id,
        user_id: order.user_id,
        market_id: market,
        origin_session_id: OriginSessionId::new(session),
        fee_schedule_version,
        fee_tier_id,
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

/// 对本次订单终结新产生的待处理归属计算真实释放，保留原事件顺序。
pub(crate) fn release_terminal_reservations(
    before: &ProductionOrderBook,
    after: &ProductionOrderBook,
    registry: &mut ReservationRegistry,
    events: Vec<EngineEvent>,
) -> Result<Vec<EngineEvent>, RejectReason> {
    use matching_domain::{CancelReason, ReservationReleaseReason};
    let terminal =
        |event: &EngineEvent| -> Result<Option<(OrderId, ReservationReleaseReason)>, RejectReason> {
            match event {
                EngineEvent::OrderCanceled {
                    order_id,
                    reason: CancelReason::MassCancel,
                    ..
                } => Ok(Some((*order_id, ReservationReleaseReason::MassCancel))),
                EngineEvent::OrderCanceled {
                    order_id,
                    reason: CancelReason::MarketClosed,
                    ..
                } => Ok(Some((*order_id, ReservationReleaseReason::MarketClosed))),
                EngineEvent::OrderCanceled {
                    order_id,
                    reason: CancelReason::KillSwitch,
                    ..
                } => Ok(Some((*order_id, ReservationReleaseReason::KillSwitch))),
                EngineEvent::OrderCanceled {
                    order_id,
                    reason: CancelReason::PriceBandRevalidation,
                    ..
                } => Ok(Some((
                    *order_id,
                    ReservationReleaseReason::PriceBandRevalidation,
                ))),
                EngineEvent::OrderExpired {
                    order_id,
                    reason: matching_domain::ExpireReason::MarketClosed,
                } => Ok(Some((*order_id, ReservationReleaseReason::MarketClosed))),
                EngineEvent::OrderExpired { order_id, .. } => {
                    Ok(Some((*order_id, ReservationReleaseReason::Expired)))
                }
                EngineEvent::OrderCanceled { .. } => Err(RejectReason::InvalidOrderCombination),
                _ => Ok(None),
            }
        };
    let mut ids = Vec::new();
    let mut reasons = BTreeMap::new();
    for event in &events {
        if let Some((order_id, reason)) = terminal(event)? {
            let state = before
                .reservation_state(order_id)
                .ok_or(RejectReason::InvalidReservation)?;
            if !registry.is_pending_r2(state.reservation_id) {
                return Err(RejectReason::InvalidReservation);
            }
            ids.push(state.reservation_id);
            if reasons.insert(order_id, reason).is_some() {
                return Err(RejectReason::InvalidReservation);
            }
        }
    }
    let plan = registry.plan_r2(&[], &ids)?;
    let next_book = after.plan_r2_shadow_update(&plan)?;
    if next_book != *after {
        return Err(RejectReason::InvalidReservation);
    }
    let mut releases = BTreeMap::new();
    for release in plan.releases() {
        releases.insert(
            release.order_id,
            release.to_event(
                *reasons
                    .get(&release.order_id)
                    .ok_or(RejectReason::InvalidReservation)?,
            ),
        );
    }
    let mut output = Vec::new();
    for event in events {
        let id = terminal(&event)?.map(|(id, _)| id);
        output.push(event);
        if let Some(id) = id
            && let Some(release) = releases.remove(&id)
        {
            output.push(release);
        }
    }
    if !releases.is_empty() {
        return Err(RejectReason::InvalidReservation);
    }
    registry
        .apply_r2(plan)
        .map_err(|_| RejectReason::InvalidReservation)?;
    Ok(output)
}

/// 将成交逐笔转换为隔离的结算候选；调用方仍须执行版本费率 R1 和持久提交。
pub(crate) fn place_candidate_with_fees(
    input: (
        PlacePlanningContext<'_>,
        &matching_domain::FeeScheduleBook,
        u64,
    ),
    place: &PlaceLimitOrder,
    book: &ProductionOrderBook,
    registry: &ReservationRegistry,
    ids: ClientOrderIds,
    cursor: &TradeIdCursor,
) -> Result<PlaceCandidate, RejectReason> {
    use matching_domain::{ReservationId, ReservationReleaseReason};
    use matching_risk::{SettlementAsset, SettlementPlanError, TradeSettlementContext};
    let (context, fees, version) = input;
    let math = context.policy.market_math();
    let (base, quote) = context.policy.assets();
    let market_id = book.market_id().ok_or(RejectReason::MarketNotFound)?;
    let mut reservation_ids = BTreeMap::new();
    for (order_id, state) in book.reservation_snapshots() {
        reservation_ids.insert(order_id, state.reservation_id);
    }
    reservation_ids.insert(place.order.order_id, place.reservation.reservation_id);
    let mut candidate = place_candidate_inner(
        context,
        place,
        book,
        registry,
        ids,
        cursor,
        Some((fees, version)),
    )?;
    let mut last_trade = BTreeMap::new();
    for (index, event) in candidate.events.iter().enumerate() {
        if let EngineEvent::Trade(trade) = event {
            last_trade.insert(trade.maker_order_id, index);
            last_trade.insert(trade.taker_order_id, index);
        }
    }
    let mut pending_releases: BTreeMap<OrderId, Vec<matching_risk::SettlementRelease>> =
        BTreeMap::new();
    let mut events = Vec::new();
    for (index, event) in std::mem::take(&mut candidate.events)
        .into_iter()
        .enumerate()
    {
        match &event {
            EngineEvent::Trade(trade) => {
                let maker = *reservation_ids
                    .get(&trade.maker_order_id)
                    .ok_or(RejectReason::InvalidReservation)?;
                let taker = *reservation_ids
                    .get(&trade.taker_order_id)
                    .ok_or(RejectReason::InvalidReservation)?;
                let mut terminal: Vec<ReservationId> = Vec::new();
                for (id, reservation) in
                    [(trade.maker_order_id, maker), (trade.taker_order_id, taker)]
                {
                    if last_trade.get(&id) == Some(&index)
                        && candidate.registry.is_pending_r2(reservation)
                    {
                        terminal.push(reservation);
                    }
                }
                let plan = candidate
                    .book
                    .plan_trade_r2_candidate(
                        &candidate.registry,
                        TradeSettlementContext {
                            market_id,
                            math,
                            base: SettlementAsset {
                                asset: base,
                                scale: math.qty_scale(),
                            },
                            quote: SettlementAsset {
                                asset: quote,
                                scale: math.quote_scale(),
                            },
                            fees,
                        },
                        trade,
                        maker,
                        taker,
                        &terminal,
                    )
                    .map_err(|error| match error {
                        SettlementPlanError::Reservation(reason)
                        | SettlementPlanError::FeeSchedule(reason) => reason,
                        SettlementPlanError::Overflow => RejectReason::ArithmeticOverflow,
                        _ => RejectReason::InvalidReservation,
                    })?;
                let (settlement, releases) = candidate
                    .book
                    .apply_trade_r2_candidate(&mut candidate.registry, plan)
                    .map_err(|_| RejectReason::InvalidReservation)?;
                events.push(event.clone());
                events.push(EngineEvent::SettlementDelta(settlement.to_event_payload()));
                for release in releases {
                    pending_releases
                        .entry(release.order_id)
                        .or_default()
                        .push(release);
                }
            }
            EngineEvent::OrderCanceled {
                order_id, reason, ..
            } => {
                if !matches!(
                    reason,
                    matching_domain::CancelReason::SelfTradePreventionMaker
                        | matching_domain::CancelReason::SelfTradePreventionTaker
                        | matching_domain::CancelReason::SelfTradePreventionBoth
                ) {
                    return Err(RejectReason::InvalidOrderCombination);
                }
                let reservation = *reservation_ids
                    .get(order_id)
                    .ok_or(RejectReason::InvalidReservation)?;
                if candidate.registry.is_pending_r2(reservation) {
                    let plan = candidate.registry.plan_r2(&[], &[reservation])?;
                    let next = candidate.book.plan_r2_shadow_update(&plan)?;
                    let releases = plan.releases().to_vec();
                    candidate
                        .registry
                        .apply_r2(plan)
                        .map_err(|_| RejectReason::InvalidReservation)?;
                    candidate.book = next;
                    pending_releases
                        .entry(*order_id)
                        .or_default()
                        .extend(releases);
                }
                events.push(event.clone());
                if let Some(releases) = pending_releases.remove(order_id) {
                    events.extend(
                        releases
                            .into_iter()
                            .map(|r| r.to_event(ReservationReleaseReason::SelfTradePrevention)),
                    );
                }
            }
            EngineEvent::OrderFilled { order_id } => {
                events.push(event.clone());
                if let Some(releases) = pending_releases.remove(order_id) {
                    events.extend(
                        releases
                            .into_iter()
                            .map(|r| r.to_event(ReservationReleaseReason::FilledRemainder)),
                    );
                }
            }
            _ => events.push(event),
        }
    }
    if !pending_releases.is_empty() {
        return Err(RejectReason::InvalidReservation);
    }
    candidate.events = events;
    Ok(candidate)
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
    fn fee_backed_multiple_makers_settle_and_release_after_lifecycle() {
        use matching_domain::{FeeAssetPolicy, FeeRounding, FeeSchedule, FeeScheduleBook, FeeTier};
        let (p, k, b, r, i, c) = setup();
        let fees = FeeScheduleBook::new(
            FeeSchedule::new(
                9,
                FeeAssetPolicy::Quote,
                [(
                    10,
                    FeeTier {
                        maker_bps: 0,
                        taker_bps: 0,
                        rounding: FeeRounding::TowardZero,
                    },
                )],
            )
            .unwrap(),
        );
        let run = |id,
                   side,
                   qty,
                   book: &ProductionOrderBook,
                   registry: &ReservationRegistry,
                   ids: &ClientOrderIds,
                   cursor: &TradeIdCursor| {
            place_candidate_with_fees(
                (
                    PlacePlanningContext {
                        policy: &p,
                        keys: &k,
                        command_id: CommandId(id),
                        session: 1,
                        now: LogicalTime::new(0),
                    },
                    &fees,
                    fees.active_version(),
                ),
                &order(id as u64, id as u64, side, qty, false),
                book,
                registry,
                ids.try_clone_preallocated().unwrap(),
                cursor,
            )
        };
        let a = run(1, Side::Sell, 2, &b, &r, &i, &c).unwrap();
        let b = run(2, Side::Sell, 2, &a.book, &a.registry, &a.ids, &a.cursor).unwrap();
        let x = run(3, Side::Buy, 3, &b.book, &b.registry, &b.ids, &b.cursor).unwrap();
        assert_eq!(
            x.events
                .iter()
                .filter(|e| matches!(e, EngineEvent::Trade(_)))
                .count(),
            2
        );
        assert_eq!(
            x.events
                .iter()
                .filter(|e| matches!(e, EngineEvent::SettlementDelta(_)))
                .count(),
            2
        );
        assert_eq!(
            x.registry.get(ReservationId(1)).unwrap().status,
            matching_risk::ReservationStatus::Released
        );
        assert_eq!(
            x.registry.get(ReservationId(3)).unwrap().status,
            matching_risk::ReservationStatus::Released
        );
        assert_eq!(
            x.registry.get(ReservationId(2)).unwrap().reserves[0].atoms,
            1
        );
        assert_eq!(
            x.book.reservation_state(OrderId(2)),
            x.registry.get(ReservationId(2))
        );
    }

    #[test]
    fn fee_backed_stp_releases_canceled_order_without_false_trade() {
        use matching_domain::{FeeAssetPolicy, FeeRounding, FeeSchedule, FeeScheduleBook, FeeTier};
        let (p, k, b, r, i, c) = setup();
        let fees = FeeScheduleBook::new(
            FeeSchedule::new(
                9,
                FeeAssetPolicy::Quote,
                [(
                    10,
                    FeeTier {
                        maker_bps: 0,
                        taker_bps: 0,
                        rounding: FeeRounding::TowardZero,
                    },
                )],
            )
            .unwrap(),
        );
        let run = |place: &PlaceLimitOrder,
                   b: &ProductionOrderBook,
                   r: &ReservationRegistry,
                   i: &ClientOrderIds,
                   c: &TradeIdCursor| {
            place_candidate_with_fees(
                (
                    PlacePlanningContext {
                        policy: &p,
                        keys: &k,
                        command_id: place.reservation.command_id,
                        session: 1,
                        now: LogicalTime::new(0),
                    },
                    &fees,
                    fees.active_version(),
                ),
                place,
                b,
                r,
                i.try_clone_preallocated().unwrap(),
                c,
            )
        };
        let a = run(&order(1, 9, Side::Sell, 2, false), &b, &r, &i, &c).unwrap();
        let mut taker = order(2, 9, Side::Buy, 1, false);
        taker.order.stp_mode = StpMode::CancelMaker;
        let x = run(&taker, &a.book, &a.registry, &a.ids, &a.cursor).unwrap();
        assert!(
            !x.events
                .iter()
                .any(|e| matches!(e, EngineEvent::Trade(_) | EngineEvent::SettlementDelta(_)))
        );
        assert!(x.events.iter().any(|e| matches!(e, EngineEvent::ReservationReleased {
            order_id: OrderId(1), reason: matching_domain::ReservationReleaseReason::SelfTradePrevention,
            released: Some(assets), .. } if assets[0].atoms == 2)));
        assert_eq!(
            x.registry.get(ReservationId(1)).unwrap().status,
            matching_risk::ReservationStatus::Released
        );
        assert_eq!(
            x.book.reservation_state(OrderId(2)),
            x.registry.get(ReservationId(2))
        );
    }

    #[test]
    fn fee_backed_candidate_rejects_stale_signed_version_without_mutation() {
        use matching_domain::{FeeAssetPolicy, FeeRounding, FeeSchedule, FeeScheduleBook, FeeTier};
        let (p, k, b, r, i, c) = setup();
        let mut fees = FeeScheduleBook::new(
            FeeSchedule::new(
                9,
                FeeAssetPolicy::Quote,
                [(
                    10,
                    FeeTier {
                        maker_bps: 0,
                        taker_bps: 0,
                        rounding: FeeRounding::TowardZero,
                    },
                )],
            )
            .unwrap(),
        );
        fees.activate(
            FeeSchedule::new(
                10,
                FeeAssetPolicy::Quote,
                [(
                    10,
                    FeeTier {
                        maker_bps: 0,
                        taker_bps: 0,
                        rounding: FeeRounding::TowardZero,
                    },
                )],
            )
            .unwrap(),
        )
        .unwrap();
        let original = (b.clone(), r.clone(), i.clone(), c.clone());
        let result = place_candidate_with_fees(
            (
                PlacePlanningContext {
                    policy: &p,
                    keys: &k,
                    command_id: CommandId(1),
                    session: 1,
                    now: LogicalTime::new(0),
                },
                &fees,
                fees.active_version(),
            ),
            &order(1, 1, Side::Sell, 2, false),
            &b,
            &r,
            i.try_clone_preallocated().unwrap(),
            &c,
        );
        assert!(matches!(result, Err(RejectReason::FeeScheduleMismatch)));
        assert_eq!((b, r, i, c), original);
    }

    #[test]
    fn managed_market_uses_its_pinned_fee_version_even_when_book_is_newer() {
        use matching_domain::{FeeAssetPolicy, FeeRounding, FeeSchedule, FeeScheduleBook, FeeTier};
        let (p, k, b, r, i, c) = setup();
        let tier = FeeTier {
            maker_bps: 0,
            taker_bps: 0,
            rounding: FeeRounding::Floor,
        };
        let mut fees =
            FeeScheduleBook::new(FeeSchedule::new(9, FeeAssetPolicy::Quote, [(10, tier)]).unwrap());
        fees.activate(FeeSchedule::new(10, FeeAssetPolicy::Quote, [(10, tier)]).unwrap())
            .unwrap();
        let accepted = place_candidate_with_fees(
            (
                PlacePlanningContext {
                    policy: &p,
                    keys: &k,
                    command_id: CommandId(1),
                    session: 1,
                    now: LogicalTime::new(0),
                },
                &fees,
                9,
            ),
            &order(1, 1, Side::Sell, 2, false),
            &b,
            &r,
            i.try_clone_preallocated().unwrap(),
            &c,
        )
        .unwrap();
        assert_eq!(
            accepted
                .registry
                .get(ReservationId(1))
                .unwrap()
                .fee_schedule_version,
            9
        );
    }

    #[test]
    fn preopen_revalidation_releases_actual_reserve_after_cancel() {
        use matching_domain::{CancelReason, ReservationReleaseReason};
        let (p, k, b, r, i, c) = setup();
        let accepted = place_candidate(
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
        let mut next_book = accepted.book.clone();
        let mut next_registry = accepted.registry.clone();
        let plan = next_book
            .plan_preopen_revalidation(
                Price::try_new(11).unwrap(),
                Price::try_new(20).unwrap(),
                &next_registry,
            )
            .unwrap();
        let (_, events) = next_book
            .apply_mass_cancel(&mut next_registry, plan)
            .unwrap();
        let result =
            release_terminal_reservations(&accepted.book, &next_book, &mut next_registry, events)
                .unwrap();
        assert!(
            matches!(result.as_slice(), [EngineEvent::OrderCanceled { reason: CancelReason::PriceBandRevalidation, .. }, EngineEvent::ReservationReleased { reason: ReservationReleaseReason::PriceBandRevalidation, released: Some(assets), .. }] if assets[0].atoms == 2)
        );
    }

    #[test]
    fn mass_cancel_releases_only_newly_canceled_reservations() {
        use matching_domain::{ReservationReleaseReason, ShardCancelScope};
        let (p, k, b, r, i, c) = setup();
        let accepted = place_candidate(
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
        let mut next_book = accepted.book.clone();
        let mut next_registry = accepted.registry.clone();
        let plan = next_book
            .plan_mass_cancel(
                ShardCancelScope::Market {
                    market_id: MarketId(1),
                },
                &next_registry,
            )
            .unwrap();
        let (_, events) = next_book
            .apply_mass_cancel(&mut next_registry, plan)
            .unwrap();
        let result =
            release_terminal_reservations(&accepted.book, &next_book, &mut next_registry, events)
                .unwrap();
        assert!(
            matches!(result.as_slice(), [EngineEvent::OrderCanceled { order_id: OrderId(1), .. },
            EngineEvent::ReservationReleased { order_id: OrderId(1), reason: ReservationReleaseReason::MassCancel,
                released: Some(assets), .. }] if assets[0].atoms == 2)
        );
        assert_eq!(
            next_registry.get(ReservationId(1)).unwrap().status,
            matching_risk::ReservationStatus::Released
        );
        assert_eq!(
            accepted.registry.get(ReservationId(1)).unwrap().status,
            matching_risk::ReservationStatus::Active
        );
    }

    #[test]
    fn end_session_releases_day_reservation_after_expiry_event() {
        use matching_domain::{ExpireReason, ReservationReleaseReason};
        let (p, k, mut book, registry, ids, cursor) = setup();
        let place = order(1, 1, Side::Sell, 2, false);
        let accepted = place_candidate(
            PlacePlanningContext {
                policy: &p,
                keys: &k,
                command_id: CommandId(1),
                session: 9,
                now: LogicalTime::new(0),
            },
            &place,
            &book,
            &registry,
            ids.try_clone_preallocated().unwrap(),
            &cursor,
        )
        .unwrap();
        let mut next_registry = accepted.registry.clone();
        book.rest_timed_with_reservation(
            place.order,
            TimeInForce::Day { session_id: 9 },
            LogicalTime::new(0),
            9,
            next_registry.get(ReservationId(1)).unwrap().clone(),
        )
        .unwrap();
        let mut next_book = book.clone();
        let plan = next_book.plan_end_session(9, 9, &next_registry).unwrap();
        let (_, events) = next_book
            .apply_mass_cancel(&mut next_registry, plan)
            .unwrap();
        let result =
            release_terminal_reservations(&book, &next_book, &mut next_registry, events).unwrap();
        assert!(
            matches!(result.as_slice(), [EngineEvent::OrderExpired { order_id: OrderId(1), reason: ExpireReason::SessionEnded },
            EngineEvent::ReservationReleased { order_id: OrderId(1), reason: ReservationReleaseReason::Expired,
                released: Some(assets), .. }] if assets[0].atoms == 2)
        );
        assert_eq!(
            next_registry.get(ReservationId(1)).unwrap().status,
            matching_risk::ReservationStatus::Released
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
