//! 撤单替换资金状态的只读计划与显式提交边界。

use crate::{
    ExpectedAmendReservation, ReservationState, ReservationStatus, RiskKeySet,
    verify_amend_reservation,
};
use matching_domain::{
    AssetReserve, CancelReplaceRejection, CommandId, MarketId, OrderId, RejectReason,
    ReservationId, ReservationTransition, UnadoptedReservation, UserId,
};
use std::collections::{BTreeMap, BTreeSet};

/// 调用方从实际旧单及可信费率、敞口计算取得的预留需求。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpectedReplacementReservation {
    /// 已分配的最终命令身份，不得从令牌反向填入。
    pub command_id: CommandId,
    /// 实际活动旧单当前绑定的预留身份。
    pub old_reservation_id: ReservationId,
    /// 调用方已核实活动位置及所有权的旧订单身份。
    pub old_order_id: OrderId,
    /// 独立新订单身份。
    pub new_order_id: OrderId,
    /// 实际旧单和新单共同的用户身份。
    pub user_id: UserId,
    /// 实际执行上下文绑定的市场身份。
    pub market_id: MarketId,
    /// 调用方读取旧预留时取得的版本。
    pub expected_version: u64,
    /// 可信本地配置锁定的新订单费率版本。
    pub fee_schedule_version: u64,
    /// 可信本地配置锁定的新订单费率层级。
    pub fee_tier_id: u32,
    /// 由真实敞口、最大费用和安全量计算的需求，严格按资产升序。
    pub required_reserves: Vec<AssetReserve>,
}

/// 只有只读验证入口能够构造的资金状态变化。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedReservationTransition {
    /// 验证时读取的旧金融快照，用于提交前漂移检测。
    before: ReservationState,
    /// 提交后接管新订单的金融快照。
    active: ReservationState,
    /// ReplaceWithNew 时保留的旧金融快照；其绑定在提交后转为 PendingR2。
    pending_r2: Option<ReservationState>,
}

/// 金融上仍为活动的预留必须恰好归属于活动订单或等待后置资金处理。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReservationBinding {
    /// 金融预留仍绑定到可见活动订单。
    ActiveOrder(OrderId),
    /// 订单已终结，金融预留等待后续 R2 结算。
    PendingR2,
}

/// 只读验证生成的整批归属变更；字段私有，调用方不能伪造或改写计划。
///
/// 完整快照同时保护金融状态和归属，提交仅改变归属，不计算或释放资金。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedBindingTransition {
    /// 验证时取得的完整登记表快照，用于提交前漂移检测。
    before: ReservationRegistry,
    /// 提交后应替换的完整预留归属映射。
    bindings: BTreeMap<ReservationId, ReservationBinding>,
}

impl PlannedReservationTransition {
    /// 提交后接管新订单的状态。
    pub fn active_state(&self) -> &ReservationState {
        &self.active
    }
    /// 提交后归属等待后置资金处理的旧快照；延续旧身份时为空。
    ///
    /// 旧金融状态完整保留，不能据此生成资金释放事件。
    pub fn pending_r2_state(&self) -> Option<&ReservationState> {
        self.pending_r2.as_ref()
    }
}

/// 计划与当前状态不一致，调用方必须按不变量错误停止，而非正常业务拒绝。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReservationApplyError {
    /// 预检状态、目标身份空缺或订单绑定已经发生漂移。
    StateChanged,
}

/// 以预留身份持有本地状态；已释放身份也保留，防止重新接管。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReservationRegistry {
    /// 以预留身份保存的完整金融状态，包含不可再次接管的已释放历史。
    states: BTreeMap<ReservationId, ReservationState>,
    /// 每个金融活动预留的唯一订单或 PendingR2 归属。
    bindings: BTreeMap<ReservationId, ReservationBinding>,
}

impl ReservationRegistry {
    /// 从已接管状态及释放历史建立唯一身份边界。
    ///
    /// 该实例持有实际状态，调用方必须保留其释放历史，不能每次仅从活动订单重建。
    pub fn new(states: impl IntoIterator<Item = ReservationState>) -> Result<Self, RejectReason> {
        Self::new_with_pending_r2(states, [])
    }

    /// 恢复金融状态及待后置资金处理的归属；其余活动预留绑定快照中的订单。
    ///
    /// 待处理身份必须存在且金融状态为活动，同一身份只能出现一次；已释放身份没有归属。
    pub fn new_with_pending_r2(
        states: impl IntoIterator<Item = ReservationState>,
        pending_r2: impl IntoIterator<Item = ReservationId>,
    ) -> Result<Self, RejectReason> {
        let mut pending = BTreeSet::new();
        for id in pending_r2 {
            if !pending.insert(id) {
                return Err(RejectReason::InvalidReservation);
            }
        }
        let mut ordered = BTreeMap::new();
        let mut bindings = BTreeMap::new();
        for state in states {
            validate_reserves(&state.reserves)?;
            if state.status == ReservationStatus::Active {
                let binding = if pending.remove(&state.reservation_id) {
                    ReservationBinding::PendingR2
                } else {
                    ReservationBinding::ActiveOrder(state.order_id)
                };
                bindings.insert(state.reservation_id, binding);
            }
            if ordered.insert(state.reservation_id, state).is_some() {
                return Err(RejectReason::InvalidReservation);
            }
        }
        if !pending.is_empty() {
            return Err(RejectReason::InvalidReservation);
        }
        let registry = Self {
            states: ordered,
            bindings,
        };
        registry.validate_bindings(&registry.bindings)?;
        Ok(registry)
    }
    /// 读取实际持有的预留状态。
    pub fn get(&self, id: ReservationId) -> Option<&ReservationState> {
        self.states.get(&id)
    }

    /// 查询归属是否等待后置资金处理；这不表示已扣减或已释放资金。
    pub fn is_pending_r2(&self, id: ReservationId) -> bool {
        self.bindings.get(&id) == Some(&ReservationBinding::PendingR2)
    }

    /// 核对按订单索引的键及活动订单归属快照，拒绝遗漏、重复或状态漂移。
    ///
    /// 待后置资金处理的预留仍然金融活动，但不得出现在活动订单索引中。
    /// 调用方在同一个单写者提交边界维护订单索引；不能将独立副本当作资金权威。
    pub fn validate_order_bindings<'a>(
        &self,
        states: impl IntoIterator<Item = (OrderId, &'a ReservationState)>,
    ) -> Result<(), RejectReason> {
        self.validate_bindings(&self.bindings)?;
        let mut seen = BTreeSet::new();
        for (order_id, state) in states {
            if order_id != state.order_id
                || state.status != ReservationStatus::Active
                || self.get(state.reservation_id) != Some(state)
                || self.bindings.get(&state.reservation_id)
                    != Some(&ReservationBinding::ActiveOrder(order_id))
                || !seen.insert(state.reservation_id)
            {
                return Err(RejectReason::InvalidReservation);
            }
        }
        if self.bindings.iter().any(|(id, binding)| {
            matches!(binding, ReservationBinding::ActiveOrder(_)) && !seen.contains(id)
        }) {
            return Err(RejectReason::InvalidReservation);
        }
        Ok(())
    }

    /// 只读规划整批终结和恢复归属；同一预留只能出现在一个输入位置。
    ///
    /// 终结输入必须仍绑定活动订单；恢复输入必须等待后置资金处理，且只能使用
    /// 当前已验证金融快照的订单身份，不能借此创建或改绑金融状态。
    /// 撤单替换必须先在不可见候选状态组合资金过渡与本批计划，再一次提交全部状态；
    /// 不得先写真实资金状态，再尝试构造可能失败的归属计划。
    pub fn plan_binding_transitions<'a>(
        &self,
        terminal: impl IntoIterator<Item = &'a ReservationState>,
        active: impl IntoIterator<Item = &'a ReservationState>,
    ) -> Result<PlannedBindingTransition, RejectReason> {
        self.validate_bindings(&self.bindings)?;
        let mut bindings = self.bindings.clone();
        let mut seen = BTreeSet::new();
        for (state, terminal) in terminal
            .into_iter()
            .map(|state| (state, true))
            .chain(active.into_iter().map(|state| (state, false)))
        {
            let expected = if terminal {
                ReservationBinding::ActiveOrder(state.order_id)
            } else {
                ReservationBinding::PendingR2
            };
            if state.status != ReservationStatus::Active
                || self.get(state.reservation_id) != Some(state)
                || self.bindings.get(&state.reservation_id) != Some(&expected)
                || !seen.insert(state.reservation_id)
            {
                return Err(RejectReason::InvalidReservation);
            }
            let target = if terminal {
                ReservationBinding::PendingR2
            } else {
                ReservationBinding::ActiveOrder(state.order_id)
            };
            bindings.insert(state.reservation_id, target);
        }
        self.validate_bindings(&bindings)?;
        Ok(PlannedBindingTransition {
            before: self.clone(),
            bindings,
        })
    }

    /// 完整快照匹配后一次提交整批归属；漂移时保留全部现状并要求调用方停止处理。
    ///
    /// 不改变金融状态、版本、覆盖量或费率，不产生事件。
    pub fn apply_binding_transition(
        &mut self,
        plan: PlannedBindingTransition,
    ) -> Result<(), ReservationApplyError> {
        if *self != plan.before {
            return Err(ReservationApplyError::StateChanged);
        }
        self.bindings = plan.bindings;
        Ok(())
    }

    fn validate_bindings(
        &self,
        bindings: &BTreeMap<ReservationId, ReservationBinding>,
    ) -> Result<(), RejectReason> {
        let mut orders = BTreeSet::new();
        for (id, state) in &self.states {
            match (state.status, bindings.get(id)) {
                (ReservationStatus::Active, Some(ReservationBinding::ActiveOrder(order)))
                    if *order == state.order_id && orders.insert(*order) => {}
                (ReservationStatus::Active, Some(ReservationBinding::PendingR2))
                | (ReservationStatus::Released, None) => {}
                _ => return Err(RejectReason::InvalidReservation),
            }
        }
        if bindings.keys().any(|id| !self.states.contains_key(id)) {
            return Err(RejectReason::InvalidReservation);
        }
        Ok(())
    }
    /// 只读验证资金过渡；任何拒绝都保留新预留的未接管清理身份。
    pub fn plan_transition(
        &self,
        keys: &RiskKeySet,
        expected: &ExpectedReplacementReservation,
        transition: &ReservationTransition,
    ) -> Result<PlannedReservationTransition, CancelReplaceRejection> {
        self.plan_inner(keys, expected, transition)
            .map_err(|reason| CancelReplaceRejection {
                reason,
                unadopted_reservation: match transition {
                    ReservationTransition::ReuseOld { .. } => None,
                    ReservationTransition::ReplaceWithNew { new_reservation } => {
                        Some(UnadoptedReservation {
                            reservation_id: new_reservation.reservation_id,
                        })
                    }
                },
            })
    }

    fn plan_inner(
        &self,
        keys: &RiskKeySet,
        expected: &ExpectedReplacementReservation,
        transition: &ReservationTransition,
    ) -> Result<PlannedReservationTransition, RejectReason> {
        let old = self
            .get(expected.old_reservation_id)
            .ok_or(RejectReason::InvalidReservation)?;
        if old.status != ReservationStatus::Active
            || self.bindings.get(&old.reservation_id)
                != Some(&ReservationBinding::ActiveOrder(expected.old_order_id))
            || old.order_id != expected.old_order_id
            || old.user_id != expected.user_id
            || old.market_id != expected.market_id
            || old.version != expected.expected_version
            || expected.old_order_id == expected.new_order_id
            || self.active_order_exists(expected.new_order_id)
        {
            return Err(RejectReason::InvalidReservation);
        }
        validate_reserves(&expected.required_reserves)?;
        match transition {
            ReservationTransition::ReuseOld { adjustment } => {
                let next_version = old
                    .version
                    .checked_add(1)
                    .ok_or(RejectReason::ArithmeticOverflow)?;
                if old.fee_schedule_version != expected.fee_schedule_version
                    || old.fee_tier_id != expected.fee_tier_id
                {
                    return Err(RejectReason::InvalidReservation);
                }
                let target = if let Some(token) = adjustment {
                    let amend_expected = ExpectedAmendReservation {
                        command_id: expected.command_id,
                        reservation_id: old.reservation_id,
                        order_id: old.order_id,
                        user_id: expected.user_id,
                        market_id: expected.market_id,
                        fee_schedule_version: expected.fee_schedule_version,
                        fee_tier_id: expected.fee_tier_id,
                        required_reserves: expected.required_reserves.clone(),
                    };
                    verify_amend_reservation(keys, old, &amend_expected, token)?;
                    verify_asset_set(&token.target_reserves, &expected.required_reserves)?;
                    token.target_reserves.to_vec()
                } else {
                    verify_coverage(&old.reserves, &expected.required_reserves)?;
                    expected.required_reserves.clone()
                };
                let mut active = old.clone();
                active.order_id = expected.new_order_id;
                active.version = next_version;
                active.reserves = target;
                Ok(PlannedReservationTransition {
                    before: old.clone(),
                    active,
                    pending_r2: None,
                })
            }
            ReservationTransition::ReplaceWithNew {
                new_reservation: token,
            } => {
                let payload = token.canonical_signing_payload()?;
                keys.verify_signature(token.risk_key_id, &payload, &token.signature)?;
                if token.command_id != expected.command_id
                    || token.order_id != expected.new_order_id
                    || token.user_id != expected.user_id
                    || token.market_id != expected.market_id
                    || token.fee_schedule_version != expected.fee_schedule_version
                    || token.fee_tier_id != expected.fee_tier_id
                    || self.states.contains_key(&token.reservation_id)
                {
                    return Err(RejectReason::InvalidReservation);
                }
                verify_coverage(&token.reserves, &expected.required_reserves)?;
                verify_asset_set(&token.reserves, &expected.required_reserves)?;
                let active = ReservationState {
                    reservation_id: token.reservation_id,
                    order_id: token.order_id,
                    user_id: token.user_id,
                    market_id: token.market_id,
                    version: 0,
                    reserves: token.reserves.to_vec(),
                    status: ReservationStatus::Active,
                    fee_schedule_version: token.fee_schedule_version,
                    fee_tier_id: token.fee_tier_id,
                };
                Ok(PlannedReservationTransition {
                    before: old.clone(),
                    active,
                    pending_r2: Some(old.clone()),
                })
            }
        }
    }

    fn active_order_exists(&self, order_id: OrderId) -> bool {
        self.bindings
            .values()
            .any(|binding| *binding == ReservationBinding::ActiveOrder(order_id))
    }
    /// 仅消费已证明计划，不产生事件；新预留版本从零开始。
    ///
    /// 延续旧预留时推进一次金融版本；改用新预留时旧金融状态完整保留，仅归属等待后置资金处理。
    ///
    /// 返回的状态漂移是必须停止处理的不变量错误，不能映射为删除旧单后的正常拒绝。
    pub fn apply_transition(
        &mut self,
        plan: PlannedReservationTransition,
    ) -> Result<(), ReservationApplyError> {
        if self.get(plan.before.reservation_id) != Some(&plan.before)
            || self.bindings.get(&plan.before.reservation_id)
                != Some(&ReservationBinding::ActiveOrder(plan.before.order_id))
            || self.active_order_exists(plan.active.order_id)
            || (plan.pending_r2.is_some() && self.states.contains_key(&plan.active.reservation_id))
        {
            return Err(ReservationApplyError::StateChanged);
        }
        // 所有不变量核对均在首次改动之前；提交不再返回正常业务拒绝。
        if let Some(pending) = plan.pending_r2 {
            self.bindings
                .insert(pending.reservation_id, ReservationBinding::PendingR2);
        }
        self.bindings.insert(
            plan.active.reservation_id,
            ReservationBinding::ActiveOrder(plan.active.order_id),
        );
        self.states.insert(plan.active.reservation_id, plan.active);
        Ok(())
    }
}

fn validate_reserves(reserves: &[AssetReserve]) -> Result<(), RejectReason> {
    if reserves.is_empty() {
        return Err(RejectReason::InvalidReservation);
    }
    let mut previous = None;
    for reserve in reserves {
        if reserve.atoms == 0 || previous.is_some_and(|asset| reserve.asset <= asset) {
            return Err(RejectReason::InvalidReservation);
        }
        previous = Some(reserve.asset);
    }
    Ok(())
}

fn verify_coverage(
    reserves: &[AssetReserve],
    required: &[AssetReserve],
) -> Result<(), RejectReason> {
    for needed in required {
        if !reserves
            .binary_search_by_key(&needed.asset, |r| r.asset)
            .is_ok_and(|i| reserves[i].atoms >= needed.atoms)
        {
            return Err(RejectReason::InsufficientReservation);
        }
    }
    Ok(())
}

fn verify_asset_set(
    reserves: &[AssetReserve],
    required: &[AssetReserve],
) -> Result<(), RejectReason> {
    if reserves.len() != required.len()
        || reserves
            .iter()
            .zip(required)
            .any(|(r, e)| r.asset != e.asset)
    {
        return Err(RejectReason::InvalidReservation);
    }
    Ok(())
}
