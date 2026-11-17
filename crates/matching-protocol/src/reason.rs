//! 业务拒绝原因的稳定数值及对外回复。
use crate::{Frame, FrameError, decode_frame, encode_frame};

/// 不丢弃未知编号的稳定原因代码；编号含义由领域契约定义。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReasonCode(u16);
impl ReasonCode {
    /// 有界引擎入口繁忙。
    pub const ENGINE_BUSY: Self = Self(5002);
    /// 包装收到的原始编号；不会把未知值映射到其他原因。
    pub const fn new(value: u16) -> Self {
        Self(value)
    }
    /// 返回线上原始编号。
    pub const fn get(self) -> u16 {
        self.0
    }
}
/// 回复帧或载荷的编码、解码失败。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RejectionCodecError {
    /// 帧编码或解码失败。
    Frame(FrameError),
    /// 类型、标志或载荷长度不符合回复模式。
    Schema,
}
/// 将稳定原因编号编码为版本一拒绝回复。
pub fn encode_rejection(
    request_id: u64,
    reason: ReasonCode,
    max_frame: usize,
) -> Result<Vec<u8>, RejectionCodecError> {
    encode_frame(
        &Frame {
            message_type: 2,
            flags: 0,
            request_id,
            payload: reason.0.to_be_bytes().to_vec(),
        },
        max_frame,
    )
    .map_err(RejectionCodecError::Frame)
}
/// 解码拒绝回复并保留未知原因编号。
pub fn decode_rejection(
    bytes: &[u8],
    max_frame: usize,
) -> Result<(u64, ReasonCode), RejectionCodecError> {
    let frame = decode_frame(bytes, max_frame).map_err(RejectionCodecError::Frame)?;
    if frame.message_type != 2 || frame.flags != 0 || frame.payload.len() != 2 {
        return Err(RejectionCodecError::Schema);
    }
    Ok((
        frame.request_id,
        ReasonCode(u16::from_be_bytes([frame.payload[0], frame.payload[1]])),
    ))
}

/// 持久接受的限价命令摘要。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Acceptance {
    /// 风险令牌和去重所绑定的命令身份。
    pub command_id: u128,
    /// 网关分配的订单身份。
    pub order_id: u128,
    /// 命令首次执行的分片序号。
    pub shard_seq: u64,
}
/// 编码包含订单身份的版本一成功回复。
pub fn encode_acceptance(
    request_id: u64,
    accepted: Acceptance,
    max_frame: usize,
) -> Result<Vec<u8>, RejectionCodecError> {
    let mut payload = Vec::with_capacity(40);
    payload.extend_from_slice(&accepted.command_id.to_be_bytes());
    payload.extend_from_slice(&accepted.order_id.to_be_bytes());
    payload.extend_from_slice(&accepted.shard_seq.to_be_bytes());
    encode_frame(
        &Frame {
            message_type: 3,
            flags: 0,
            request_id,
            payload,
        },
        max_frame,
    )
    .map_err(RejectionCodecError::Frame)
}
/// 解码成功回复，拒绝未知标志和不匹配的载荷长度。
pub fn decode_acceptance(
    bytes: &[u8],
    max_frame: usize,
) -> Result<(u64, Acceptance), RejectionCodecError> {
    let frame = decode_frame(bytes, max_frame).map_err(RejectionCodecError::Frame)?;
    if frame.message_type != 3 || frame.flags != 0 || frame.payload.len() != 40 {
        return Err(RejectionCodecError::Schema);
    }
    let p = &frame.payload;
    Ok((
        frame.request_id,
        Acceptance {
            command_id: u128::from_be_bytes(
                p[..16]
                    .try_into()
                    .map_err(|_| RejectionCodecError::Schema)?,
            ),
            order_id: u128::from_be_bytes(
                p[16..32]
                    .try_into()
                    .map_err(|_| RejectionCodecError::Schema)?,
            ),
            shard_seq: u64::from_be_bytes(
                p[32..40]
                    .try_into()
                    .map_err(|_| RejectionCodecError::Schema)?,
            ),
        },
    ))
}
