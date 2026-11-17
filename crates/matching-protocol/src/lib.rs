//! 网关使用的有界版本化二进制帧。

/// 当前线协议版本。
pub const VERSION: u16 = 1;
/// 帧头字节数，不包括载荷和校验码。
pub const HEADER_LEN: usize = 22;
const MAGIC: &[u8; 4] = b"TIDE";
const TRAILER_LEN: usize = 4;

/// 完整帧的业务类型、标志、请求身份和载荷。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    /// 消息类型的协议编号。
    pub message_type: u16,
    /// 消息标志位；含义由消息类型定义。
    pub flags: u16,
    /// 调用方用于关联响应的身份。
    pub request_id: u64,
    /// 帧的原始载荷；解码时会检查帧长度。
    pub payload: Vec<u8>,
}

/// 帧编码或解码失败。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameError {
    /// 帧长度超过配置上限或整数范围。
    TooLarge,
    /// 输入不足以包含声明的完整帧。
    Truncated,
    /// 完整帧之后存在多余数据。
    TrailingBytes,
    /// 魔数无效。
    Magic,
    /// 版本不受支持。
    Version,
    /// CRC32C 校验失败。
    Checksum,
}

/// 编码一帧；长度上限包含头部、载荷与校验码。
pub fn encode_frame(frame: &Frame, max_frame_length: usize) -> Result<Vec<u8>, FrameError> {
    let len = u32::try_from(frame.payload.len()).map_err(|_| FrameError::TooLarge)?;
    let total = HEADER_LEN
        .checked_add(frame.payload.len())
        .and_then(|n| n.checked_add(TRAILER_LEN))
        .ok_or(FrameError::TooLarge)?;
    if total > max_frame_length {
        return Err(FrameError::TooLarge);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(total)
        .map_err(|_| FrameError::TooLarge)?;
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&VERSION.to_be_bytes());
    bytes.extend_from_slice(&frame.message_type.to_be_bytes());
    bytes.extend_from_slice(&frame.flags.to_be_bytes());
    bytes.extend_from_slice(&frame.request_id.to_be_bytes());
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(&frame.payload);
    bytes.extend_from_slice(&crc32c(&bytes).to_be_bytes());
    Ok(bytes)
}

/// 从固定长度帧头确定整个帧的字节数；调用方应先调用本函数再读取载荷。
pub fn frame_length(bytes: &[u8], max_frame_length: usize) -> Result<usize, FrameError> {
    if bytes.len() < HEADER_LEN {
        return Err(FrameError::Truncated);
    }
    if &bytes[..4] != MAGIC {
        return Err(FrameError::Magic);
    }
    if u16::from_be_bytes([bytes[4], bytes[5]]) != VERSION {
        return Err(FrameError::Version);
    }
    let length = u32::from_be_bytes(
        bytes[18..22]
            .try_into()
            .map_err(|_| FrameError::Truncated)?,
    );
    let length = usize::try_from(length).map_err(|_| FrameError::TooLarge)?;
    let total = HEADER_LEN
        .checked_add(length)
        .and_then(|n| n.checked_add(TRAILER_LEN))
        .ok_or(FrameError::TooLarge)?;
    if total > max_frame_length {
        return Err(FrameError::TooLarge);
    }
    Ok(total)
}

/// 严格解码单帧；先检查声明长度和上限，再分配载荷。
pub fn decode_frame(bytes: &[u8], max_frame_length: usize) -> Result<Frame, FrameError> {
    let total = frame_length(bytes, max_frame_length)?;
    let length = total - HEADER_LEN - TRAILER_LEN;
    if bytes.len() < total {
        return Err(FrameError::Truncated);
    }
    if bytes.len() > total {
        return Err(FrameError::TrailingBytes);
    }
    let expected = u32::from_be_bytes(
        bytes[total - 4..]
            .try_into()
            .map_err(|_| FrameError::Truncated)?,
    );
    if crc32c(&bytes[..total - 4]) != expected {
        return Err(FrameError::Checksum);
    }
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(length)
        .map_err(|_| FrameError::TooLarge)?;
    payload.extend_from_slice(&bytes[HEADER_LEN..total - 4]);
    Ok(Frame {
        message_type: u16::from_be_bytes([bytes[6], bytes[7]]),
        flags: u16::from_be_bytes([bytes[8], bytes[9]]),
        request_id: u64::from_be_bytes(
            bytes[10..18]
                .try_into()
                .map_err(|_| FrameError::Truncated)?,
        ),
        payload,
    })
}

/// 使用 Castagnoli 多项式检测帧传输损坏。
fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0x82f6_3b78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// 由线性一致控制平面分配的网关身份与纪元。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaseIdentity {
    /// 租约所属集群。
    pub cluster: u16,
    /// 控制平面授予的网关节点。
    pub node: u16,
    /// 控制平面授予的节点纪元。
    pub epoch: u32,
}

/// 外部租约权威；必须跨所有实例原子分配不重复计数器，并在失效时拒绝。
///
/// 实现须在返回前持久保留计数器，或确保重启获得新纪元；不能由调用方自选计数器。
pub trait LeaseAuthority {
    /// 检查当前租约与计数器是否可用；这只是瞬时状态，不替代分配时复核。
    fn is_ready(&self, identity: LeaseIdentity) -> bool;
    /// 验证租约并原子保留该纪元内一个此前未使用的计数器。
    fn next_counter(&mut self, identity: LeaseIdentity) -> Result<u64, OrderIdError>;
}

/// 订单身份分配失败。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderIdError {
    /// 租约不可用或已经失效。
    LeaseUnavailable,
    /// 当前纪元的计数器空间耗尽。
    Exhausted,
}

/// 不依赖时钟的租约式订单身份分配器。
pub struct LeaseOrderIdAllocator<A> {
    /// 参与订单身份编码的租约身份。
    identity: LeaseIdentity,
    /// 负责校验租约和原子保留计数器的可信权威。
    authority: A,
}

impl<A: LeaseAuthority> LeaseOrderIdAllocator<A> {
    /// 使用可信权威构造；权威负责跨实例和重启的计数器唯一性。
    pub fn new(identity: LeaseIdentity, authority: A) -> Self {
        Self {
            identity,
            authority,
        }
    }

    /// 返回租约权威报告的当前可用状态；分配时仍必须重新检查。
    pub fn is_ready(&self) -> bool {
        self.authority.is_ready(self.identity)
    }

    /// 验证租约并生成身份；失效或耗尽时不生成身份。
    pub fn allocate(&mut self) -> Result<u128, OrderIdError> {
        let counter = self.authority.next_counter(self.identity)?;
        Ok((u128::from(self.identity.cluster) << 112)
            | (u128::from(self.identity.node) << 96)
            | (u128::from(self.identity.epoch) << 64)
            | u128::from(counter))
    }
}

mod rate;
pub use rate::{BucketConfig, GatewayRateLimiters, RateError, RateLimiter};

mod place;
/// 网关外部限价请求的稳定二进制模式。
pub use place::{
    PlaceCodecError, PlaceRequest, PlaceSide, StpPolicy, decode_place_request, encode_place_request,
};

mod reason;
/// 固定原因编号的拒绝回复与包含订单身份的成功回复。
pub use reason::{
    Acceptance, ReasonCode, RejectionCodecError, decode_acceptance, decode_rejection,
    encode_acceptance, encode_rejection,
};
