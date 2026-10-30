//! 撤单替换的独立参考组合计划及一次性提交。
//!
//! 旧单逻辑过滤、资金过渡与普通下单均只作用于可丢弃候选，真实提交不再撮合或拒绝。

use super::{ReferenceOrderBook, ReferencePlaceContext, ReferencePlaceOrder};
use matching_domain::{
    CancelReason, CancelReplaceOrder, CancelReplaceRejection, EngineEvent, IcebergOrder,
    LimitOrder, OrderRemaining, Qty, RejectReason, ReplacementOrderKind, StopOrder,
};
use matching_risk::{
    ExpectedReplacementReservation, ReservationApplyError, ReservationRegistry, ReservationStatus,
    RiskKeySet,
};

/// 完整固定原簿、真实金融登记表和预演后态的不可伪造参考计划。
///
/// 后态包含已验证的资金过渡、所有终结挂单与新到订单的整批 PendingR2 归属。
/// 旧单金融状态仅在延续预留时作已授权调整；终结归属不等于金融释放。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReferenceCancelReplacePlan {
    /// 计划生成时的完整参考簿快照，用于提交前漂移检测。
    before_book: ReferenceOrderBook,
    /// 计划生成时的完整金融登记表快照，用于提交前漂移检测。
    before_registry: ReservationRegistry,
    /// 移除旧单并完成替换预演后的参考簿状态。
    after_book: ReferenceOrderBook,
    /// 包含旧单、终结 maker 与新单归属的提交后登记表状态。
    after_registry: ReservationRegistry,
    /// 先撤旧单、后正常下单的原序事件。
    events: Vec<EngineEvent>,
}

impl ReferenceOrderBook {
    /// 先定位真实活动旧单，再以独立过滤视图完成所有可能拒绝的验证。
    ///
    /// `expected` 的命令身份、市场、费率及完整资金上界必须来自调用方真实执行配置，
    /// 不得从令牌填入或猜测。它与实际旧影子及独立新单身份再次交叉核对。
    /// `context` 延续正常参考下单的显式时间、数值及成交身份契约。
    /// 不读取网络、墙上时钟或生产簿，不产生任何资金释放事件。
    pub fn plan_cancel_replace(
        &self,
        request: &CancelReplaceOrder,
        expected: &ExpectedReplacementReservation,
        registry: &ReservationRegistry,
        keys: &RiskKeySet,
        context: ReferencePlaceContext<'_>,
    ) -> Result<ReferenceCancelReplacePlan, CancelReplaceRejection> {
        self.plan_cancel_replace_inner(request, expected, registry, keys, context)
            .map_err(|reason| request.rejection(reason))
    }

    /// 完整核对簿及登记表前态后，一次安装已验证的组合后态和有序事件。
    ///
    /// 首次真实写入后无撮合、资金校验、正常拒绝或回滚；漂移必须按不变量错误停止。
    pub fn apply_cancel_replace(
        &mut self,
        registry: &mut ReservationRegistry,
        plan: ReferenceCancelReplacePlan,
    ) -> Result<Vec<EngineEvent>, ReservationApplyError> {
        if *self != plan.before_book || *registry != plan.before_registry {
            return Err(ReservationApplyError::StateChanged);
        }
        *self = plan.after_book;
        *registry = plan.after_registry;
        Ok(plan.events)
    }

    fn plan_cancel_replace_inner(
        &self,
        request: &CancelReplaceOrder,
        expected: &ExpectedReplacementReservation,
        registry: &ReservationRegistry,
        keys: &RiskKeySet,
        context: ReferencePlaceContext<'_>,
    ) -> Result<ReferenceCancelReplacePlan, RejectReason> {
        // 旧活动位置和真实所有权先于新单结构及令牌检查，避免身份探测改变拒绝语义。
        let (old_user, old_remaining) = if let Some(old) = self
            .orders
            .iter()
            .find(|old| old.original_order().order_id == request.old_order_id)
        {
            (old.original_order().user_id, old.total_remaining())
        } else if let Some((old, _)) = self.stop_order(request.old_order_id) {
            match old {
                StopOrder::Limit(old) => (old.user_id, old.qty.get()),
                StopOrder::Market(old) => {
                    if old.user_id != request.user_id || request.market_id != expected.market_id {
                        return Err(RejectReason::OrderNotFound);
                    }
                    return Err(RejectReason::InvalidOrderCombination);
                }
            }
        } else {
            return Err(RejectReason::OrderNotFound);
        };
        if old_user != request.user_id || request.market_id != expected.market_id {
            return Err(RejectReason::OrderNotFound);
        }
        request.validate_structure()?;
        self.check_duplicate(request.replacement.order_id)?;
        let old_state = self
            .reservations
            .get(&request.old_order_id)
            .ok_or(RejectReason::InvalidReservation)?;
        if old_state.status != ReservationStatus::Active
            || old_state.order_id != request.old_order_id
            || old_state.user_id != old_user
            || old_state.market_id != expected.market_id
            || old_state.reservation_id != expected.old_reservation_id
            || old_state.version != expected.expected_version
            || expected.old_order_id != request.old_order_id
            || expected.new_order_id != request.replacement.order_id
            || expected.user_id != old_user
        {
            return Err(RejectReason::InvalidReservation);
        }
        if let Some((StopOrder::Limit(old), _)) = self.stop_order(request.old_order_id)
            && (old.reservation_id != old_state.reservation_id
                || old.fee_schedule_version != old_state.fee_schedule_version)
        {
            return Err(RejectReason::InvalidReservation);
        }
        registry
            .validate_order_bindings(self.reservations.iter().map(|(id, state)| (*id, state)))?;

        let replacement = &request.replacement;
        let order = LimitOrder {
            order_id: replacement.order_id,
            user_id: replacement.user_id,
            stp_key: replacement.stp_key,
            stp_mode: replacement.stp_mode,
            side: replacement.side,
            price: replacement.price,
            qty: replacement.qty,
        };
        let place = match replacement.kind {
            ReplacementOrderKind::Limit { tif, flags } => ReferencePlaceOrder::Limit {
                order,
                time_in_force: tif,
                post_only: flags.post_only,
            },
            ReplacementOrderKind::Iceberg {
                peak_size, flags, ..
            } => ReferencePlaceOrder::Iceberg {
                order: IcebergOrder::try_new(order, peak_size)?,
                post_only: flags.post_only,
            },
        };

        // BookWithout 是唯一撮合输入；过滤包括冰山隐藏量、补量与止损到期载荷。
        let mut after_book = self.book_without(request.old_order_id)?.into_candidate();
        after_book.reservations.remove(&request.old_order_id);
        // 普通下单与撤单替换共用完整只读预演；结构、时间、初始及逐笔本金、
        // 撮合、补量、余量挂单、优先级和终结挂单校验均先于资金过渡验证。
        let place_preflight = after_book.preflight_place(place, context)?;
        let transition = registry
            .plan_transition(keys, expected, &request.reservation_transition)
            .map_err(|rejection| rejection.reason)?;
        let active = transition.active_state().clone();
        let mut after_registry = registry.clone();
        after_registry
            .apply_transition(transition)
            .expect("fresh reference financial transition must match its registry");
        // 通用正常下单负责每一张全成挂单及不入簿新单的归属批次。
        // 任何校验失败仅丢弃候选，不接触真实旧单或真实登记表。
        let place_plan =
            after_book.bind_place_preflight(place_preflight, active, &after_registry)?;
        let mut events = vec![EngineEvent::OrderCanceled {
            order_id: request.old_order_id,
            remaining: OrderRemaining::Base(Qty::try_new(old_remaining)?),
            reason: CancelReason::CancelReplace,
        }];
        events.extend(
            after_book
                .apply_place_with_registry(&mut after_registry, place_plan)
                .expect("fresh reference place plan must match its complete candidate"),
        );
        after_registry.validate_order_bindings(
            after_book
                .reservations
                .iter()
                .map(|(id, state)| (*id, state)),
        )?;
        Ok(ReferenceCancelReplacePlan {
            before_book: self.clone(),
            before_registry: registry.clone(),
            after_book,
            after_registry,
            events,
        })
    }
}
