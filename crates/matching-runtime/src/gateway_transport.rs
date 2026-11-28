//! 已认证会话的有界同步字节流适配器。
use matching_domain::{CommandId, OrderId, ReservationToken};
use std::io::{self, Read, Write};
use std::time::Duration;

#[cfg(test)]
#[path = "gateway_transport_tests.rs"]
mod tests;

/// 连接处理或断连清理失败。
#[derive(Debug)]
pub enum GatewayTransportError {
    /// 输入输出或截断错误。
    Io(io::Error),
    /// 帧长度或完整性错误。
    Frame(matching_protocol::FrameError),
    /// 网关认证后准入或风险授权错误。
    Gateway(crate::GatewayError),
    /// 分片正式命令发生错误，连接处理无法继续。
    Formal(crate::FormalError),
    /// 返回的业务状态不符合限价请求。
    Outcome,
    /// 结果未知，客户端须保留身份重试。
    ResponseUnknown,
    /// 网关共享状态锁已损坏，不能继续准入。
    SharedState,
    /// 已提交结果的外围交接失败；命令结果必须按原身份核对。
    Delivery(io::Error),
    /// 健康检查与网关的分片集合不一致。
    HealthConfiguration,
    /// 会话撤单未完成；日志保留未确认目标。
    Disconnect(crate::SessionDispatchError),
    /// 同时发生原始处理错误和断连清理错误。
    Cleanup {
        /// 连接处理阶段的原始错误。
        processing: Box<GatewayTransportError>,
        /// 关闭或撤单阶段的错误。
        cleanup: Box<GatewayTransportError>,
    },
}

/// 所有连接共享的本机意图权威与全局限流器。
pub struct GatewayShared {
    /// 只保护短暂的意图登记、令牌登记和准入操作，不保护订单簿。
    state: std::sync::Mutex<GatewaySharedState>,
}
/// 共用状态的内部所有权。
struct GatewaySharedState {
    /// 独占的持久身份与令牌日志。
    store: crate::GatewayIntentStore,
    /// 所有连接共用的分层限流桶。
    limits: matching_protocol::GatewayRateLimiters,
    /// 使用统一基准和单位的本地非阻塞单调时钟。
    now: Box<dyn FnMut() -> u64 + Send>,
}
impl GatewayShared {
    /// 接管权威意图表、限流器和所有连接共用的本地非阻塞单调时钟。
    pub fn new(
        store: crate::GatewayIntentStore,
        limits: matching_protocol::GatewayRateLimiters,
        now: impl FnMut() -> u64 + Send + 'static,
    ) -> Self {
        Self {
            state: std::sync::Mutex::new(GatewaySharedState {
                store,
                limits,
                now: Box::new(now),
            }),
        }
    }
}

/// 已认证会话的传输适配器；认证、风险授权和身份分配由可信外围提供。
///
/// 流只由本调用独占，调用方须设置读取和写入超时；客户端不直接接触分片句柄。
/// 缺失响应或超时可能表示业务已经执行，后续请求必须复用幂等键。
pub struct GatewayTransport<'a, A, R> {
    /// 绑定认证身份和断连日志的会话。
    pub session: &'a mut crate::GatewaySession,
    /// 跨连接共用的本机意图表与分层限流器。
    pub shared: &'a GatewayShared,
    /// 所有连接共用的节点健康检查，失去就绪时禁止新的业务准入。
    pub health: &'a crate::Health,
    /// 所有持久目标分片的消息句柄。
    pub shards: &'a [crate::ShardHandle],
    /// 包含头部和校验码的最大帧长度。
    pub max_frame: usize,
    /// 单次分片回复及断连清理的最大等待时间。
    pub timeout: Duration,
    /// 为首次意图从已保留的本地额度分配身份；不得在共享锁内访问远程服务。
    pub allocate: A,
    /// 按 CommandId 持久幂等签发风险授权；崩溃重试必须返回逐字段相同的令牌。
    /// 入队前失败或结果未知的预留由风险服务按同一身份协调，网关不伪造释放。
    pub authorize: R,
}
impl<'a, A, R> GatewayTransport<'a, A, R>
where
    A: FnMut() -> Result<(CommandId, OrderId), crate::GatewayError>,
    R: FnMut(&crate::PreparedPlace) -> Result<ReservationToken, crate::GatewayError>,
{
    /// 处理完整请求帧，连接结束或出错后总是尝试排空并执行断连策略。
    pub fn serve(&mut self, stream: &mut (impl Read + Write)) -> Result<(), GatewayTransportError> {
        self.serve_with(stream, |_| Ok(()))
    }

    /// 在连接线程交接已提交结果后再回复；交接失败时结果未知且结束连接。
    ///
    /// 回调不在分片线程执行，调用方负责有界背压、跨连接排序和失败恢复。
    /// 此回调不是持久事件日志，发生失败或进程崩溃须从权威持久前缀恢复。
    pub fn serve_with(
        &mut self,
        stream: &mut (impl Read + Write),
        mut on_committed: impl FnMut(&crate::FormalExecution) -> io::Result<()>,
    ) -> Result<(), GatewayTransportError> {
        let namespace = self
            .shards
            .first()
            .and_then(crate::ShardHandle::control_namespace);
        let result = if self.health.covers(self.shards)
            && self
                .shards
                .iter()
                .all(|s| s.control_namespace() == namespace)
        {
            self.receive_loop(stream, &mut on_committed)
        } else {
            Err(GatewayTransportError::HealthConfiguration)
        };
        let cleanup = self.cleanup();
        match (result, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(processing), Err(cleanup)) => Err(GatewayTransportError::Cleanup {
                processing: Box::new(processing),
                cleanup: Box::new(cleanup),
            }),
        }
    }
    /// 在统一期限内重试入口繁忙或结果未知；未完成目标保留在持久日志中。
    fn cleanup(&mut self) -> Result<(), GatewayTransportError> {
        let deadline = std::time::Instant::now()
            .checked_add(self.timeout)
            .ok_or(GatewayTransportError::ResponseUnknown)?;
        loop {
            let remaining = deadline
                .checked_duration_since(std::time::Instant::now())
                .ok_or(GatewayTransportError::ResponseUnknown)?;
            match self.session.disconnect(self.shards, remaining) {
                Ok(()) => break,
                Err(crate::GatewayError::Ingress(crate::ShardError::Busy)) => {
                    std::thread::yield_now()
                }
                Err(error) => return Err(GatewayTransportError::Gateway(error)),
            }
        }
        loop {
            let remaining = deadline
                .checked_duration_since(std::time::Instant::now())
                .ok_or(GatewayTransportError::ResponseUnknown)?;
            match self.session.dispatch_all_cancels(self.shards, remaining) {
                Ok(()) => return Ok(()),
                Err(
                    crate::SessionDispatchError::Ingress(crate::ShardError::Busy)
                    | crate::SessionDispatchError::OutcomeUnknown
                    | crate::SessionDispatchError::Formal(crate::FormalError::Rejected(
                        matching_domain::RejectReason::StaleRouteEpoch,
                    )),
                ) => std::thread::yield_now(),
                Err(error) => return Err(GatewayTransportError::Disconnect(error)),
            }
        }
    }
    /// 在帧头限长后读取载荷，按请求顺序向分片提交。
    fn receive_loop(
        &mut self,
        stream: &mut (impl Read + Write),
        on_committed: &mut impl FnMut(&crate::FormalExecution) -> io::Result<()>,
    ) -> Result<(), GatewayTransportError> {
        'requests: loop {
            let mut header = [0_u8; matching_protocol::HEADER_LEN];
            match stream.read(&mut header[..1]) {
                Ok(0) => return Ok(()),
                Ok(_) => (),
                Err(error) => return Err(GatewayTransportError::Io(error)),
            }
            stream
                .read_exact(&mut header[1..])
                .map_err(GatewayTransportError::Io)?;
            let length = matching_protocol::frame_length(&header, self.max_frame)
                .map_err(GatewayTransportError::Frame)?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(length)
                .map_err(|_| GatewayTransportError::Io(io::Error::other("请求帧内存不足")))?;
            bytes.extend_from_slice(&header);
            bytes.resize(length, 0);
            stream
                .read_exact(&mut bytes[matching_protocol::HEADER_LEN..])
                .map_err(GatewayTransportError::Io)?;
            let frame = matching_protocol::decode_frame(&bytes, self.max_frame)
                .map_err(GatewayTransportError::Frame)?;
            if !self.health.ready() {
                self.reject_not_ready(stream, frame.request_id)?;
                continue;
            }
            // 完整帧通过长度与 CRC 检查后，以会话认证身份读取栅栏。检查在风险授权和全局身份分配之前。
            let admission = if matches!(
                frame.message_type,
                matching_protocol::TYPE_PLACE
                    | matching_protocol::TYPE_AMEND
                    | matching_protocol::TYPE_CANCEL_REPLACE
            ) {
                let handle = self
                    .shards
                    .first()
                    .ok_or(GatewayTransportError::HealthConfiguration)?;
                let status = handle
                    .admission_status(self.session.authenticated_user())
                    .map_err(|e| GatewayTransportError::Gateway(crate::GatewayError::Ingress(e)))?;
                let rejection = status.reason.or_else(|| {
                    (frame.message_type != matching_protocol::TYPE_PLACE)
                        .then_some(matching_domain::RejectReason::InvalidOrderCombination)
                });
                if let Some(rejected) = rejection {
                    self.reject_request(stream, frame.request_id, rejected)?;
                    continue;
                }
                status
            } else {
                Default::default()
            };
            let allocation = &mut self.allocate;
            let prepared = {
                let mut state = self
                    .shared
                    .state
                    .lock()
                    .map_err(|_| GatewayTransportError::SharedState)?;
                let now = (state.now)();
                let GatewaySharedState { store, limits, .. } = &mut *state;
                self.session.prepare_wire_at_epoch(
                    &bytes,
                    self.max_frame,
                    store,
                    limits,
                    (now, admission),
                    allocation,
                )
            };
            let (request_id, prepared) = match prepared {
                Ok(value) => value,
                Err(error) => {
                    if let Some(reason) = error.ingress_reason() {
                        let reply = matching_protocol::encode_rejection(
                            frame.request_id,
                            matching_protocol::ReasonCode::new(reason as u16),
                            self.max_frame,
                        )
                        .map_err(|_| GatewayTransportError::Outcome)?;
                        stream
                            .write_all(&reply)
                            .map_err(GatewayTransportError::Io)?;
                        continue;
                    }
                    return Err(GatewayTransportError::Gateway(error));
                }
            };
            let market = prepared.intent().market;
            let Some(mut shard) = self.select_owner(market, request_id, stream)? else {
                continue;
            };
            let existing = {
                let state = self
                    .shared
                    .state
                    .lock()
                    .map_err(|_| GatewayTransportError::SharedState)?;
                state.store.existing_authorization(&prepared)
            };
            let token_result = match existing {
                Ok(Some(token)) => Ok(token),
                Ok(None) => match (self.authorize)(&prepared) {
                    Ok(token) => {
                        let mut state = self
                            .shared
                            .state
                            .lock()
                            .map_err(|_| GatewayTransportError::SharedState)?;
                        state.store.authorize(&prepared, || Ok(token))
                    }
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            };
            let token = match token_result {
                Ok(token) => token,
                Err(error) => {
                    if let Some(reason) = error.ingress_reason() {
                        let bytes = matching_protocol::encode_rejection(
                            request_id,
                            matching_protocol::ReasonCode::new(reason as u16),
                            self.max_frame,
                        )
                        .map_err(|_| GatewayTransportError::Outcome)?;
                        stream
                            .write_all(&bytes)
                            .map_err(GatewayTransportError::Io)?;
                        continue;
                    }
                    return Err(GatewayTransportError::Gateway(error));
                }
            };
            let mut stale_retries = 0_u8;
            let result = loop {
                // 每次投递尝试在连接线程检查权威，不持共享锁。已准入的旧请求由分片本地纪元隔开。
                let current = shard
                    .admission_status(self.session.authenticated_user())
                    .map_err(|e| GatewayTransportError::Gateway(crate::GatewayError::Ingress(e)))?;
                let rejection = current
                    .rejection(prepared.intent().user_control_epoch)
                    .or_else(|| {
                        (current.global_epoch() != admission.global_epoch())
                            .then_some(matching_domain::RejectReason::ControlFenceActive)
                    });
                if let Some(reason) = rejection {
                    self.reject_request(stream, request_id, reason)?;
                    continue 'requests;
                }
                let submitted = {
                    let mut state = self
                        .shared
                        .state
                        .lock()
                        .map_err(|_| GatewayTransportError::SharedState)?;
                    let client_seq = state
                        .store
                        .client_seq(prepared.command_id())
                        .ok_or(GatewayTransportError::Outcome)?;
                    let now = (state.now)();
                    self.health.admit(|| {
                        self.session.submit_admitted_place(
                            &prepared,
                            token.clone(),
                            (client_seq, admission.global_epoch()),
                            &mut state.limits,
                            now,
                            shard,
                        )
                    })
                };
                let reply = match submitted {
                    Some(Ok(receiver)) => receiver
                        .recv_timeout(self.timeout)
                        .map_err(|_| GatewayTransportError::ResponseUnknown)?,
                    None => {
                        self.reject_not_ready(stream, request_id)?;
                        continue 'requests;
                    }
                    Some(Err(crate::GatewayError::Route)) => Err(crate::FormalError::Rejected(
                        matching_domain::RejectReason::StaleRouteEpoch,
                    )),
                    Some(Err(error)) => {
                        if let Some(reason) = error.ingress_reason() {
                            let bytes = matching_protocol::encode_rejection(
                                request_id,
                                matching_protocol::ReasonCode::new(reason as u16),
                                self.max_frame,
                            )
                            .map_err(|_| GatewayTransportError::Outcome)?;
                            stream
                                .write_all(&bytes)
                                .map_err(GatewayTransportError::Io)?;
                            continue 'requests;
                        }
                        return Err(GatewayTransportError::Gateway(error));
                    }
                };
                match reply {
                    Ok(value) => break value,
                    Err(crate::FormalError::Rejected(
                        matching_domain::RejectReason::StaleRouteEpoch,
                    )) if stale_retries < 2 => {
                        stale_retries = stale_retries.checked_add(1).expect("有界路由重试计数");
                        let Some(current) = self.select_owner(market, request_id, stream)? else {
                            continue 'requests;
                        };
                        shard = current;
                        continue;
                    }
                    Err(crate::FormalError::Rejected(reason)) => {
                        let bytes = matching_protocol::encode_rejection(
                            request_id,
                            matching_protocol::ReasonCode::new(reason as u16),
                            self.max_frame,
                        )
                        .map_err(|_| GatewayTransportError::Outcome)?;
                        stream
                            .write_all(&bytes)
                            .map_err(GatewayTransportError::Io)?;
                        continue 'requests;
                    }
                    Err(error) => return Err(GatewayTransportError::Formal(error)),
                }
            };
            let delivery =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| on_committed(&result)))
                    .unwrap_or_else(|_| Err(io::Error::other("事件交接异常")));
            if let Err(error) = delivery {
                self.health
                    .set_healthy(crate::HealthDependency::Persistence, false);
                return Err(GatewayTransportError::Delivery(error));
            }
            match result.outcome {
                crate::FormalOutcome::Placed(order_id) => {
                    let bytes = matching_protocol::encode_acceptance(
                        request_id,
                        matching_protocol::Acceptance {
                            command_id: result.command_id.0,
                            order_id: order_id.0,
                            shard_seq: result.seq,
                        },
                        self.max_frame,
                    )
                    .map_err(|_| GatewayTransportError::Outcome)?;
                    stream
                        .write_all(&bytes)
                        .map_err(GatewayTransportError::Io)?;
                }
                crate::FormalOutcome::Rejected(reason) => {
                    let bytes = matching_protocol::encode_rejection(
                        request_id,
                        matching_protocol::ReasonCode::new(reason as u16),
                        self.max_frame,
                    )
                    .map_err(|_| GatewayTransportError::Outcome)?;
                    stream
                        .write_all(&bytes)
                        .map_err(GatewayTransportError::Io)?;
                }
                _ => return Err(GatewayTransportError::Outcome),
            }
        }
    }

    /// 从分片公布的路由选择活动所有者。首次授权前及旧纪元重试时，若市场正在迁移就向客户端回复拒绝。
    fn select_owner(
        &self,
        market: matching_domain::MarketId,
        request_id: u64,
        stream: &mut impl Write,
    ) -> Result<Option<&'a crate::ShardHandle>, GatewayTransportError> {
        let mut reason = matching_domain::RejectReason::StaleRouteEpoch;
        for shard in self.shards {
            let routes = shard.routes().map_err(|error| {
                GatewayTransportError::Gateway(crate::GatewayError::Ingress(error))
            })?;
            if let Some(route) = routes.get(market)
                && route.shard_id == shard.shard_id()
            {
                match routes.validate(market, shard.shard_id(), route.route_epoch) {
                    Ok(()) => return Ok(Some(shard)),
                    Err(rejected) => {
                        reason = rejected;
                        break;
                    }
                }
            }
        }
        let bytes = matching_protocol::encode_rejection(
            request_id,
            matching_protocol::ReasonCode::new(reason as u16),
            self.max_frame,
        )
        .map_err(|_| GatewayTransportError::Outcome)?;
        stream
            .write_all(&bytes)
            .map_err(GatewayTransportError::Io)?;
        Ok(None)
    }

    /// 对尚未进入分片的未就绪请求返回稳定过载原因。
    ///
    /// 初次检查发生在登记意图前；最终准入检查可能发生在登记意图后。
    fn reject_not_ready(
        &self,
        stream: &mut impl Write,
        request_id: u64,
    ) -> Result<(), GatewayTransportError> {
        self.reject_request(
            stream,
            request_id,
            matching_domain::RejectReason::EngineBusy,
        )
    }

    /// 将前置拒绝作为关联到原请求身份的稳定线协议响应返回。
    fn reject_request(
        &self,
        stream: &mut impl Write,
        request_id: u64,
        reason: matching_domain::RejectReason,
    ) -> Result<(), GatewayTransportError> {
        let reply = matching_protocol::encode_rejection(
            request_id,
            matching_protocol::ReasonCode::new(reason as u16),
            self.max_frame,
        )
        .map_err(|_| GatewayTransportError::Outcome)?;
        stream.write_all(&reply).map_err(GatewayTransportError::Io)
    }
}
