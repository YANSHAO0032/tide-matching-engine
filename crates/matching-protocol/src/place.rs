//! 对外限价请求的明确字段和数值边界。
use crate::{Frame, FrameError, decode_frame, encode_frame};

/// 限价请求的买卖方向。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlaceSide {
    /// 买入。
    Buy,
    /// 卖出。
    Sell,
}
/// 吃单时的自成交处理策略。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StpPolicy {
    /// 不启用。
    None,
    /// 终结吃单。
    CancelTaker,
    /// 撤销挂单。
    CancelMaker,
    /// 两者均终结。
    CancelBoth,
}
/// 认证用户提交的规范限价意图；订单和命令身份由网关确定。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlaceRequest {
    /// 用户提供的幂等键。
    pub key: u128,
    /// 目标市场。
    pub market: u32,
    /// 原始价格整数，必须大于零。
    pub price: i64,
    /// 原始基础数量整数，必须大于零。
    pub qty: u64,
    /// 账户授予的自成交分组；生产部署须由可信账户策略复核。
    pub stp_key: u64,
    /// 用户控制纪元。
    pub user_control_epoch: u64,
    /// 买卖方向。
    pub side: PlaceSide,
    /// 自成交策略。
    pub stp: StpPolicy,
    /// 只挂单标志。
    pub post_only: bool,
    /// 可选客户端订单身份。
    pub client_order_id: Option<String>,
}
/// 请求帧或消息载荷无效。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlaceCodecError {
    /// 帧校验失败。
    Frame(FrameError),
    /// 类型、标志、字段或字符串不符合版本化模式。
    Schema,
    /// 字符串或整个帧超出配置的长度上限。
    TooLarge,
    /// 价格不是严格正值。
    InvalidPrice,
    /// 数量不是严格正值。
    InvalidQty,
}
const TYPE_PLACE: u16 = 1;
const FIXED: usize = 58;

/// 将请求编码为版本一大端帧；请求身份只用于响应关联。
pub fn encode_place_request(
    request_id: u64,
    request: &PlaceRequest,
    max_frame: usize,
    max_client_id: usize,
) -> Result<Vec<u8>, PlaceCodecError> {
    let id = request.client_order_id.as_deref().unwrap_or("").as_bytes();
    if id.len() > max_client_id || id.len() > usize::from(u16::MAX) {
        return Err(PlaceCodecError::TooLarge);
    }
    if request.price <= 0 {
        return Err(PlaceCodecError::InvalidPrice);
    }
    if request.qty == 0 {
        return Err(PlaceCodecError::InvalidQty);
    }
    let total = crate::HEADER_LEN
        .checked_add(FIXED)
        .and_then(|n| n.checked_add(id.len()))
        .and_then(|n| n.checked_add(4))
        .ok_or(PlaceCodecError::TooLarge)?;
    if total > max_frame {
        return Err(PlaceCodecError::TooLarge);
    }
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(FIXED + id.len())
        .map_err(|_| PlaceCodecError::TooLarge)?;
    payload.extend_from_slice(&request.key.to_be_bytes());
    payload.extend_from_slice(&request.market.to_be_bytes());
    payload.extend_from_slice(&request.price.to_be_bytes());
    payload.extend_from_slice(&request.qty.to_be_bytes());
    payload.extend_from_slice(&request.stp_key.to_be_bytes());
    payload.extend_from_slice(&request.user_control_epoch.to_be_bytes());
    payload.push(match request.side {
        PlaceSide::Buy => 0,
        PlaceSide::Sell => 1,
    });
    payload.push(match request.stp {
        StpPolicy::None => 0,
        StpPolicy::CancelTaker => 1,
        StpPolicy::CancelMaker => 2,
        StpPolicy::CancelBoth => 3,
    });
    payload.push(u8::from(request.post_only));
    payload.push(u8::from(request.client_order_id.is_some()));
    payload.extend_from_slice(
        &u16::try_from(id.len())
            .map_err(|_| PlaceCodecError::TooLarge)?
            .to_be_bytes(),
    );
    payload.extend_from_slice(id);
    encode_frame(
        &Frame {
            message_type: TYPE_PLACE,
            flags: 0,
            request_id,
            payload,
        },
        max_frame,
    )
    .map_err(PlaceCodecError::Frame)
}

/// 验证完整帧及请求载荷；无效枚举、非正值、尾随字节和超长字符串均拒绝。
pub fn decode_place_request(
    bytes: &[u8],
    max_frame: usize,
    max_client_id: usize,
) -> Result<(u64, PlaceRequest), PlaceCodecError> {
    let frame = decode_frame(bytes, max_frame).map_err(PlaceCodecError::Frame)?;
    let p = &frame.payload;
    if frame.message_type != TYPE_PLACE || frame.flags != 0 || p.len() < FIXED {
        return Err(PlaceCodecError::Schema);
    }
    let read_u64 = |range: std::ops::Range<usize>| -> Result<u64, PlaceCodecError> {
        Ok(u64::from_be_bytes(
            p[range].try_into().map_err(|_| PlaceCodecError::Schema)?,
        ))
    };
    let key = u128::from_be_bytes(p[..16].try_into().map_err(|_| PlaceCodecError::Schema)?);
    let market = u32::from_be_bytes(p[16..20].try_into().map_err(|_| PlaceCodecError::Schema)?);
    let price = i64::from_be_bytes(p[20..28].try_into().map_err(|_| PlaceCodecError::Schema)?);
    let qty = read_u64(28..36)?;
    let stp_key = read_u64(36..44)?;
    let user_control_epoch = read_u64(44..52)?;
    let side = match p[52] {
        0 => PlaceSide::Buy,
        1 => PlaceSide::Sell,
        _ => return Err(PlaceCodecError::Schema),
    };
    let stp = match p[53] {
        0 => StpPolicy::None,
        1 => StpPolicy::CancelTaker,
        2 => StpPolicy::CancelMaker,
        3 => StpPolicy::CancelBoth,
        _ => return Err(PlaceCodecError::Schema),
    };
    let post_only = match p[54] {
        0 => false,
        1 => true,
        _ => return Err(PlaceCodecError::Schema),
    };
    let size = usize::from(u16::from_be_bytes([p[56], p[57]]));
    if size > max_client_id {
        return Err(PlaceCodecError::TooLarge);
    }
    if FIXED.checked_add(size) != Some(p.len()) {
        return Err(PlaceCodecError::Schema);
    }
    let client_order_id = match p[55] {
        0 if size == 0 => None,
        1 => Some(
            std::str::from_utf8(&p[FIXED..])
                .map_err(|_| PlaceCodecError::Schema)?
                .to_owned(),
        ),
        _ => return Err(PlaceCodecError::Schema),
    };
    if price <= 0 {
        return Err(PlaceCodecError::InvalidPrice);
    }
    if qty == 0 {
        return Err(PlaceCodecError::InvalidQty);
    }
    Ok((
        frame.request_id,
        PlaceRequest {
            key,
            market,
            price,
            qty,
            stp_key,
            user_control_epoch,
            side,
            stp,
            post_only,
            client_order_id,
        },
    ))
}
