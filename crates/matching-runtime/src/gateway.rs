//! 网关会话认证、命令身份绑定和有界消息准入。
use matching_domain::{
    Command, CommandEnvelope, CommandId, LimitOrder, MarketId, PlaceLimitOrder, ReservationToken,
    UserId,
};
use matching_protocol::{GatewayRateLimiters, RateError};
use std::{
    sync::mpsc::Receiver,
    time::{Duration, Instant},
};

/// 认证服务授予的用户和自成交分组。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GatewayPrincipal {
    /// 凭据所属用户。
    pub user: UserId,
    /// 由可信账户策略授予的自成交分组。
    pub stp_key: matching_domain::StpKey,
}
/// 外部认证服务；成功时返回凭据绑定的身份和账户策略。
pub trait GatewayAuthenticator {
    /// 校验凭据，失败返回空；调用方负责密钥安全和重放保护。
    fn authenticate(&self, key: u64, secret: &[u8]) -> Option<GatewayPrincipal>;
}

/// 风险授权前固定的限价业务意图。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlaceIntent {
    /// 目标市场。
    pub market: MarketId,
    /// 订单内容及已分配的订单身份。
    pub order: LimitOrder,
    /// 用户客户端订单身份。
    pub client_order_id: Option<String>,
    /// 用户控制版本。
    pub user_control_epoch: u64,
    /// 是否仅允许挂单。
    pub post_only: bool,
}

/// 无法由外部伪造字段的已绑定意图。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedPlace {
    /// 用户域内幂等键。
    key: u128,
    /// 风险授权和分片去重共用的稳定命令身份。
    command_id: CommandId,
    /// 与该身份绑定的完整限价意图。
    intent: PlaceIntent,
    /// 身份是否已写入并登记于持久意图水位。
    durable: bool,
}
impl PreparedPlace {
    /// 指示身份绑定已持久登记，仅供网关协调逻辑检查。
    pub(crate) fn is_durable(&self) -> bool {
        self.durable
    }
    /// 返回风险授权必须绑定的最终命令身份。
    pub fn command_id(&self) -> CommandId {
        self.command_id
    }
    /// 返回风险授权所需的固定意图。
    pub fn intent(&self) -> &PlaceIntent {
        &self.intent
    }
}

/// 网关会话准入或断连失败。
#[derive(Debug)]
pub enum GatewayError {
    /// 密钥认证失败或用户不一致。
    Unauthorized,
    /// 会话已关闭或正在排空。
    Closed,
    /// 幂等键或命令身份与已有意图冲突。
    Conflict,
    /// 身份表已满或字段超出配置。
    Capacity,
    /// 市场不属于启用前持久登记的目标，或路由与句柄不匹配。
    Route,
    /// 风险令牌未绑定到本次固定意图。
    Reservation,
    /// 速率限制器拒绝请求。
    Rate(RateError),
    /// 分片入口拒绝。
    Ingress(crate::ShardError),
    /// 排空超时或回复丢失；会话仍保持关闭准入。
    Drain,
    /// 会话持久日志失败。
    Journal(std::io::Error),
    /// 外部请求帧或载荷无效。
    Protocol(matching_protocol::PlaceCodecError),
    /// 全局身份分配器不可用或耗尽。
    Allocation,
    /// 意图身份尚未持久登记，不允许进入分片。
    IdentityNotDurable,
    /// 风险服务或其他可信业务校验返回的稳定拒绝原因。
    Rejected(matching_domain::RejectReason),
}
impl GatewayError {
    /// 将过载映射为稳定原因；时钟、存储及其他故障不伪装成繁忙。
    pub fn ingress_reason(&self) -> Option<matching_domain::RejectReason> {
        match self {
            Self::Capacity | Self::Rate(RateError::Limited | RateError::Capacity) => {
                Some(matching_domain::RejectReason::EngineBusy)
            }
            Self::Ingress(error) => error.ingress_reason(),
            Self::Route => Some(matching_domain::RejectReason::StaleRouteEpoch),
            Self::Reservation => Some(matching_domain::RejectReason::InvalidReservation),
            Self::Protocol(matching_protocol::PlaceCodecError::InvalidPrice) => {
                Some(matching_domain::RejectReason::InvalidPrice)
            }
            Self::Protocol(matching_protocol::PlaceCodecError::InvalidQty) => {
                Some(matching_domain::RejectReason::InvalidQty)
            }
            Self::Rejected(reason) => Some(*reason),
            _ => None,
        }
    }
}

/// 单个接入会话的串行准入状态；订单簿仅通过分片消息访问。
pub struct GatewaySession {
    /// 由认证服务绑定的用户身份。
    user: UserId,
    /// 账户策略授权的自成交分组。
    stp_key: matching_domain::StpKey,
    /// 参与限流的认证密钥身份。
    key: u64,
    /// 该会话的本机断连控制日志。
    journal: crate::SessionControlJournal,
    /// 当前会话内已绑定的有限意图。
    entries: Vec<PreparedPlace>,
    /// 本会话最多登记的意图数。
    max_entries: usize,
    /// 客户端订单身份的字节上限。
    max_client_id: usize,
    /// 一旦开始关闭即阻止新的会话准入。
    closing: bool,
}
impl GatewaySession {
    /// 在授权前同步登记意图身份；跨会话重试复用已持久化的身份。
    pub fn prepare_wire_durable(
        &mut self,
        bytes: &[u8],
        max_frame: usize,
        store: &mut crate::GatewayIntentStore,
        limiter: &mut GatewayRateLimiters,
        now: u64,
        allocate: impl FnOnce() -> Result<(CommandId, matching_domain::OrderId), GatewayError>,
    ) -> Result<(u64, PreparedPlace), GatewayError> {
        if self.closing || !self.journal.is_open() {
            return Err(GatewayError::Closed);
        }
        let (request_id, request) =
            matching_protocol::decode_place_request(bytes, max_frame, self.max_client_id)
                .map_err(GatewayError::Protocol)?;
        if matching_domain::StpKey::new(request.stp_key) != self.stp_key {
            return Err(GatewayError::Unauthorized);
        }
        if !self
            .journal
            .targets()
            .iter()
            .any(|t| t.market == MarketId(request.market))
        {
            return Err(GatewayError::Route);
        }
        if self.entries.len() == self.max_entries
            && !self.entries.iter().any(|e| e.key == request.key)
        {
            return Err(GatewayError::Capacity);
        }
        let canonical =
            matching_protocol::encode_place_request(0, &request, max_frame, self.max_client_id)
                .map_err(GatewayError::Protocol)?;
        limiter
            .check_prepare(self.key, self.user.0, now)
            .map_err(GatewayError::Rate)?;
        let (command, order) = store.bind(self.user, request.key, &canonical, allocate)?;
        let (_, mut prepared) = self.prepare_wire(bytes, max_frame, command, order)?;
        if prepared.command_id != command || prepared.intent.order.order_id != order {
            return Err(GatewayError::Conflict);
        }
        prepared.durable = true;
        let entry = self
            .entries
            .iter_mut()
            .find(|e| e.key == request.key)
            .ok_or(GatewayError::Conflict)?;
        entry.durable = true;
        Ok((request_id, prepared))
    }
    /// 仅在新的有效意图首次出现时调用身份分配器；重试不分配任何身份。
    pub fn prepare_wire_with(
        &mut self,
        bytes: &[u8],
        max_frame: usize,
        allocate: impl FnOnce() -> Result<(CommandId, matching_domain::OrderId), GatewayError>,
    ) -> Result<(u64, PreparedPlace), GatewayError> {
        if self.closing || !self.journal.is_open() {
            return Err(GatewayError::Closed);
        }
        let (_, request) =
            matching_protocol::decode_place_request(bytes, max_frame, self.max_client_id)
                .map_err(GatewayError::Protocol)?;
        if matching_domain::StpKey::new(request.stp_key) != self.stp_key {
            return Err(GatewayError::Unauthorized);
        }
        if !self
            .journal
            .targets()
            .iter()
            .any(|t| t.market == MarketId(request.market))
        {
            return Err(GatewayError::Route);
        }
        let identities = match self.entries.iter().find(|entry| entry.key == request.key) {
            Some(entry) => (entry.command_id, entry.intent.order.order_id),
            None => {
                if self.entries.len() == self.max_entries {
                    return Err(GatewayError::Capacity);
                }
                allocate()?
            }
        };
        self.prepare_wire(bytes, max_frame, identities.0, identities.1)
    }
    /// 解码外部请求并在授权前绑定身份；重试沿用首次订单和命令身份。
    pub fn prepare_wire(
        &mut self,
        bytes: &[u8],
        max_frame: usize,
        candidate: CommandId,
        order_id: matching_domain::OrderId,
    ) -> Result<(u64, PreparedPlace), GatewayError> {
        let (request_id, request) =
            matching_protocol::decode_place_request(bytes, max_frame, self.max_client_id)
                .map_err(GatewayError::Protocol)?;
        let order_id = self
            .entries
            .iter()
            .find(|e| e.key == request.key)
            .map_or(order_id, |e| e.intent.order.order_id);
        let side = match request.side {
            matching_protocol::PlaceSide::Buy => matching_domain::Side::Buy,
            matching_protocol::PlaceSide::Sell => matching_domain::Side::Sell,
        };
        let stp_mode = match request.stp {
            matching_protocol::StpPolicy::None => matching_domain::StpMode::None,
            matching_protocol::StpPolicy::CancelTaker => matching_domain::StpMode::CancelTaker,
            matching_protocol::StpPolicy::CancelMaker => matching_domain::StpMode::CancelMaker,
            matching_protocol::StpPolicy::CancelBoth => matching_domain::StpMode::CancelBoth,
        };
        let intent = PlaceIntent {
            market: MarketId(request.market),
            order: LimitOrder {
                order_id,
                user_id: self.user,
                stp_key: matching_domain::StpKey::new(request.stp_key),
                stp_mode,
                side,
                price: matching_domain::Price::try_new(request.price).map_err(|_| {
                    GatewayError::Protocol(matching_protocol::PlaceCodecError::Schema)
                })?,
                qty: matching_domain::Qty::try_new(request.qty).map_err(|_| {
                    GatewayError::Protocol(matching_protocol::PlaceCodecError::Schema)
                })?,
            },
            client_order_id: request.client_order_id,
            user_control_epoch: request.user_control_epoch,
            post_only: request.post_only,
        };
        Ok((request_id, self.prepare(request.key, candidate, intent)?))
    }
    /// 认证用户并接管已持久登记的开放会话。
    pub fn open(
        auth: &impl GatewayAuthenticator,
        key: u64,
        secret: &[u8],
        journal: crate::SessionControlJournal,
        max_entries: usize,
        max_client_id: usize,
    ) -> Result<Self, GatewayError> {
        let principal = auth
            .authenticate(key, secret)
            .ok_or(GatewayError::Unauthorized)?;
        if principal.user != journal.user() {
            return Err(GatewayError::Unauthorized);
        }
        if !journal.is_open() {
            return Err(GatewayError::Closed);
        }
        if max_entries == 0 || max_client_id == 0 {
            return Err(GatewayError::Capacity);
        }
        Ok(Self {
            user: principal.user,
            stp_key: principal.stp_key,
            key,
            journal,
            entries: Vec::new(),
            max_entries,
            max_client_id,
            closing: false,
        })
    }
    /// 在获取风险授权之前绑定幂等键、已分配命令身份及完整订单意图。
    ///
    /// `candidate` 和订单身份由可信全局分配器提供；窗口不淘汰，满时拒绝。
    pub fn prepare(
        &mut self,
        key: u128,
        candidate: CommandId,
        intent: PlaceIntent,
    ) -> Result<PreparedPlace, GatewayError> {
        if self.closing || !self.journal.is_open() {
            return Err(GatewayError::Closed);
        }
        if intent.order.user_id != self.user || intent.order.stp_key != self.stp_key {
            return Err(GatewayError::Unauthorized);
        }
        if !self
            .journal
            .targets()
            .iter()
            .any(|t| t.market == intent.market)
        {
            return Err(GatewayError::Route);
        }
        if intent
            .client_order_id
            .as_ref()
            .is_some_and(|id| id.len() > self.max_client_id)
        {
            return Err(GatewayError::Capacity);
        }
        if let Some(existing) = self.entries.iter().find(|e| e.key == key) {
            return if existing.intent == intent {
                Ok(existing.clone())
            } else {
                Err(GatewayError::Conflict)
            };
        }
        if self.entries.iter().any(|e| e.command_id == candidate) {
            return Err(GatewayError::Conflict);
        }
        if self.entries.len() == self.max_entries {
            return Err(GatewayError::Capacity);
        }
        self.entries
            .try_reserve(1)
            .map_err(|_| GatewayError::Capacity)?;
        let prepared = PreparedPlace {
            key,
            command_id: candidate,
            intent,
            durable: false,
        };
        self.entries.push(prepared.clone());
        Ok(prepared)
    }
    /// 以已绑定身份和风险令牌向唯一目标分片有界提交；成功入队不表示执行成功。
    pub fn submit_place(
        &mut self,
        prepared: &PreparedPlace,
        token: ReservationToken,
        client_seq: u64,
        limiter: &mut GatewayRateLimiters,
        now: u64,
        shard: &crate::ShardHandle,
    ) -> Result<Receiver<Result<crate::FormalExecution, crate::FormalError>>, GatewayError> {
        if self.closing || !self.journal.is_open() {
            return Err(GatewayError::Closed);
        }
        if !self.entries.contains(prepared) {
            return Err(GatewayError::Conflict);
        }
        if !prepared.durable {
            return Err(GatewayError::IdentityNotDurable);
        }
        let intent = &prepared.intent;
        if token.command_id != prepared.command_id
            || token.order_id != intent.order.order_id
            || token.user_id != self.user
            || token.market_id != intent.market
        {
            return Err(GatewayError::Reservation);
        }
        let target = self
            .journal
            .targets()
            .iter()
            .find(|t| t.market == intent.market)
            .ok_or(GatewayError::Route)?;
        if !shard.has_durable_formal()
            || shard
                .routes()
                .validate(intent.market, shard.shard_id(), target.route_epoch)
                .is_err()
        {
            return Err(GatewayError::Route);
        }
        limiter
            .check_submit(self.key, self.user.0, now)
            .map_err(GatewayError::Rate)?;
        let response = shard
            .submit_command(CommandEnvelope {
                command_id: prepared.command_id,
                session_id: self.journal.session().0,
                client_seq,
                market_id: Some(intent.market),
                user_id: Some(self.user),
                route_epoch: Some(target.route_epoch),
                client_timestamp: None,
                received_at_unix_ns: None,
                payload: Command::PlaceLimit(Box::new(PlaceLimitOrder {
                    order: intent.order.clone(),
                    client_order_id: intent.client_order_id.clone(),
                    user_control_epoch: intent.user_control_epoch,
                    post_only: intent.post_only,
                    reservation: token,
                })),
            })
            .map_err(GatewayError::Ingress)?;
        Ok(response)
    }
    /// 关闭准入并对所有目标分片排空；全部确认后持久记录断连。
    pub fn disconnect(
        &mut self,
        shards: &[crate::ShardHandle],
        timeout: Duration,
    ) -> Result<(), GatewayError> {
        self.closing = true;
        let end = Instant::now()
            .checked_add(timeout)
            .ok_or(GatewayError::Drain)?;
        for target in self.journal.targets() {
            let shard = shards
                .iter()
                .find(|shard| {
                    shard.has_durable_formal()
                        && shard
                            .routes()
                            .validate(target.market, shard.shard_id(), target.route_epoch)
                            .is_ok()
                })
                .ok_or(GatewayError::Route)?;
            let reply = shard
                .barrier(u128::from(target.market.0))
                .map_err(GatewayError::Ingress)?;
            let remaining = end
                .checked_duration_since(Instant::now())
                .ok_or(GatewayError::Drain)?;
            let (id, _) = reply
                .recv_timeout(remaining)
                .map_err(|_| GatewayError::Drain)?;
            if id != u128::from(target.market.0) {
                return Err(GatewayError::Drain);
            }
        }
        self.journal.disconnect().map_err(GatewayError::Journal)
    }
    /// 对已关闭的会话派发一个撤单目标，并在持久成功后记录确认。
    pub fn dispatch_cancel(
        &mut self,
        market: MarketId,
        shard: &crate::ShardHandle,
        timeout: Duration,
    ) -> Result<matching_domain::ShardMassCancelResult, crate::SessionDispatchError> {
        crate::dispatch_session_cancel(&mut self.journal, market, shard, timeout)
    }
    /// 依次完成所有已关闭会话的待处理目标，失败时保留尚未确认的目标。
    pub fn dispatch_all_cancels(
        &mut self,
        shards: &[crate::ShardHandle],
        timeout: Duration,
    ) -> Result<(), crate::SessionDispatchError> {
        crate::dispatch_session_cancels(&mut self.journal, shards, timeout)
    }
}
