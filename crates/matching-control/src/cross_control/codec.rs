//! 按控制键绑定 CRC 的大端存储格式。恢复时校验稳定子命令和确认范围。
use super::*;
use std::io::Read;

/// 范围已发布的纪元和开关，以及尚未发布的控制操作。
#[derive(Default)]
pub(super) struct Scope {
    /// 已发布的范围纪元，停写升级时可初始化为该用户所有分片历史下限的最大值。
    pub epoch: u64,
    /// 在最近一次已发布状态中是否禁止新单。
    pub killed: bool,
    /// 正在持有该范围围栏的父命令。
    pub fence: Option<CommandId>,
}

fn framed(key: &str, mut value: Vec<u8>) -> Vec<u8> {
    let mut input = key.as_bytes().to_vec();
    input.extend_from_slice(&value);
    value.extend_from_slice(&matching_persistence::crc32c(&input).to_be_bytes());
    value
}
fn payload<'a>(key: &str, bytes: &'a [u8], magic: &[u8; 4]) -> io::Result<&'a [u8]> {
    let crc_offset = bytes
        .len()
        .checked_sub(4)
        .ok_or_else(|| invalid("控制记录缺少校验字段"))?;
    let (data, tail) = bytes.split_at(crc_offset);
    let body = data
        .strip_prefix(magic.as_slice())
        .ok_or_else(|| invalid("控制记录格式错误"))?;
    let mut input = key.as_bytes().to_vec();
    input.extend_from_slice(data);
    if matching_persistence::crc32c(&input).to_be_bytes() != tail {
        return Err(invalid("控制记录校验失败"));
    }
    Ok(body)
}
fn take<const N: usize>(bytes: &mut &[u8]) -> io::Result<[u8; N]> {
    let mut out = [0; N];
    bytes.read_exact(&mut out)?;
    Ok(out)
}
fn flag(bytes: &mut &[u8]) -> io::Result<bool> {
    match take::<1>(bytes)? {
        [0] => Ok(false),
        [1] => Ok(true),
        _ => Err(invalid("控制布尔字段无效")),
    }
}
fn finish(bytes: &[u8]) -> io::Result<()> {
    if bytes.is_empty() {
        Ok(())
    } else {
        Err(invalid("控制记录存在尾随数据"))
    }
}

impl Scope {
    /// 编码已发布的范围状态及当前围栏身份。
    pub(super) fn encode(&self, key: &str) -> Vec<u8> {
        let mut bytes = b"SCP1".to_vec();
        bytes.extend_from_slice(&self.epoch.to_be_bytes());
        bytes.push(u8::from(self.killed));
        bytes.push(u8::from(self.fence.is_some()));
        if let Some(parent) = self.fence {
            bytes.extend_from_slice(&parent.0.to_be_bytes());
        }
        framed(key, bytes)
    }
    /// 调用方已核对范围见证。初次使用采用零纪元，已有记录须校验键、格式和内容。
    pub(super) fn decode(key: &str, value: Option<&[u8]>) -> io::Result<Self> {
        let Some(value) = value else {
            return Ok(Self::default());
        };
        let mut bytes = payload(key, value, b"SCP1")?;
        let epoch = u64::from_be_bytes(take(&mut bytes)?);
        let killed = flag(&mut bytes)?;
        let fence = if flag(&mut bytes)? {
            Some(CommandId(u128::from_be_bytes(take(&mut bytes)?)))
        } else {
            None
        };
        finish(bytes)?;
        if epoch == 0 && killed {
            return Err(invalid("控制范围缺少已发布纪元"));
        }
        Ok(Self {
            epoch,
            killed,
            fence,
        })
    }
}
/// 按父命令身份排序的未终结操作索引。
pub(super) fn encode_index(key: &str, ids: &BTreeSet<CommandId>) -> Vec<u8> {
    let mut bytes = b"CIX1".to_vec();
    for id in ids {
        bytes.extend_from_slice(&id.0.to_be_bytes());
    }
    framed(key, bytes)
}
/// 校验索引容量、校验和和规范排序。
pub(super) fn decode_index(key: &str, value: &[u8]) -> io::Result<BTreeSet<CommandId>> {
    let mut bytes = payload(key, value, b"CIX1")?;
    if !bytes.len().is_multiple_of(16) || bytes.len() / 16 > MAX_PENDING {
        return Err(invalid("未完成控制操作索引长度错误"));
    }
    let mut result = BTreeSet::new();
    while !bytes.is_empty() {
        let id = CommandId(u128::from_be_bytes(take(&mut bytes)?));
        if result.last().is_some_and(|last| *last >= id) {
            return Err(invalid("控制操作索引不严格递增"));
        }
        result.insert(id);
    }
    Ok(result)
}
/// 已固化的目标分片严格按身份升序写入，没有在线状态参与编码。
pub(super) fn encode_members(key: &str, members: impl Iterator<Item = ShardId>) -> Vec<u8> {
    let mut bytes = b"CTP1".to_vec();
    for member in members {
        bytes.extend_from_slice(&member.0.to_be_bytes());
    }
    framed(key, bytes)
}
const PHASES: [ControlPhase; 8] = [
    ControlPhase::Prepared,
    ControlPhase::Fencing,
    ControlPhase::Fanout,
    ControlPhase::AwaitingAcks,
    ControlPhase::Committed,
    ControlPhase::Released,
    ControlPhase::Done,
    ControlPhase::Aborted,
];

/// 写入阶段、版本、稳定子命令及首次分片回执。
pub(super) fn encode_record(key: &str, op: &ControlOperation) -> io::Result<Vec<u8>> {
    let mut bytes = b"COP1".to_vec();
    bytes.extend_from_slice(&op.parent.0.to_be_bytes());
    match op.kind {
        ControlKind::UserMassCancel(user) => {
            bytes.push(1);
            bytes.extend_from_slice(&user.0.to_be_bytes());
        }
        ControlKind::GlobalMassCancel => bytes.push(2),
        ControlKind::UserKill {
            user,
            active,
            cancel_all,
        } => {
            bytes.push(3);
            bytes.extend_from_slice(&user.0.to_be_bytes());
            bytes.extend_from_slice(&[u8::from(active), u8::from(cancel_all)]);
        }
        ControlKind::GlobalKill { active, cancel_all } => {
            bytes.extend_from_slice(&[4, u8::from(active), u8::from(cancel_all)]);
        }
    }
    bytes.push(
        u8::try_from(
            PHASES
                .iter()
                .position(|p| *p == op.phase)
                .ok_or_else(|| invalid("未知控制阶段"))?,
        )
        .map_err(|_| invalid("控制阶段越界"))?,
    );
    bytes.extend_from_slice(&op.epoch.to_be_bytes());
    bytes.extend_from_slice(&op.generation.to_be_bytes());
    bytes.extend_from_slice(
        &u16::try_from(op.targets.len())
            .map_err(|_| invalid("目标分片过多"))?
            .to_be_bytes(),
    );
    for (shard, id) in &op.targets {
        bytes.extend_from_slice(&shard.0.to_be_bytes());
        bytes.extend_from_slice(&id.0.to_be_bytes());
        let ack = op.acks.get(shard);
        bytes.push(u8::from(ack.is_some()));
        if let Some(ack) = ack {
            bytes.extend_from_slice(&ack.shard_seq.to_be_bytes());
            bytes.extend_from_slice(&ack.canceled.to_be_bytes());
        }
    }
    Ok(framed(key, bytes))
}
/// 恢复时重新派生全部子命令并核对阶段与回执集合。
pub(super) fn decode_record(
    key: &str,
    parent: CommandId,
    value: &[u8],
) -> io::Result<ControlOperation> {
    let mut bytes = payload(key, value, b"COP1")?;
    if CommandId(u128::from_be_bytes(take(&mut bytes)?)) != parent {
        return Err(invalid("控制记录与父命令不符"));
    }
    let kind = match take::<1>(&mut bytes)? {
        [1] => ControlKind::UserMassCancel(UserId(u64::from_be_bytes(take(&mut bytes)?))),
        [2] => ControlKind::GlobalMassCancel,
        [3] => ControlKind::UserKill {
            user: UserId(u64::from_be_bytes(take(&mut bytes)?)),
            active: flag(&mut bytes)?,
            cancel_all: flag(&mut bytes)?,
        },
        [4] => ControlKind::GlobalKill {
            active: flag(&mut bytes)?,
            cancel_all: flag(&mut bytes)?,
        },
        _ => return Err(invalid("未知控制类型")),
    };
    let [stage] = take(&mut bytes)?;
    let phase = *PHASES
        .get(usize::from(stage))
        .ok_or_else(|| invalid("未知控制阶段"))?;
    let epoch = u64::from_be_bytes(take(&mut bytes)?);
    let generation = u64::from_be_bytes(take(&mut bytes)?);
    let count = usize::from(u16::from_be_bytes(take(&mut bytes)?));
    if !(1..=4096).contains(&count) {
        return Err(invalid("控制目标数量越界"));
    }
    let mut targets = BTreeMap::new();
    let mut acks = BTreeMap::new();
    for _ in 0..count {
        let shard = ShardId(u16::from_be_bytes(take(&mut bytes)?));
        let id = CommandId(u128::from_be_bytes(take(&mut bytes)?));
        if targets
            .last_key_value()
            .is_some_and(|(last, _)| *last >= shard)
        {
            return Err(invalid("控制目标不严格递增"));
        }
        targets.insert(shard, id);
        if flag(&mut bytes)? {
            let shard_seq = u64::from_be_bytes(take(&mut bytes)?);
            let canceled = u64::from_be_bytes(take(&mut bytes)?);
            if shard_seq == 0 {
                return Err(invalid("分片确认序号为零"));
            }
            acks.insert(
                shard,
                ControlAck {
                    shard_seq,
                    canceled,
                },
            );
        }
    }
    finish(bytes)?;
    let mut record =
        ControlOperation::prepared(parent, kind, &targets.keys().copied().collect::<Vec<_>>())?;
    let valid_epoch = match phase {
        ControlPhase::Prepared => epoch == 0,
        ControlPhase::Aborted => true,
        _ => epoch > 0,
    };
    let valid_acks = match phase {
        ControlPhase::Committed | ControlPhase::Released | ControlPhase::Done => {
            acks.len() == count
        }
        ControlPhase::AwaitingAcks => true,
        _ => acks.is_empty(),
    };
    let transitions_before_acks: u64 = match phase {
        ControlPhase::Prepared => 1,
        ControlPhase::Fencing => 2,
        ControlPhase::Fanout => 3,
        ControlPhase::AwaitingAcks => 4,
        ControlPhase::Committed => 5,
        ControlPhase::Released => 6,
        ControlPhase::Done => 7,
        ControlPhase::Aborted if epoch == 0 => 2,
        ControlPhase::Aborted => 3,
    };
    let expected_generation = transitions_before_acks
        .checked_add(u64::try_from(acks.len()).map_err(|_| invalid("回执数越界"))?)
        .ok_or_else(|| invalid("控制版本越界"))?;
    // 已预留纪元且尚未授权发送时，可在 Fencing 或 Fanout 安全中止。
    let valid_generation = generation == expected_generation
        || (phase == ControlPhase::Aborted && epoch > 0 && generation == 4);
    if targets != record.targets || !valid_epoch || !valid_acks || !valid_generation {
        return Err(invalid("控制记录的身份、纪元、版本或确认集合错误"));
    }
    record.phase = phase;
    record.epoch = epoch;
    record.generation = generation;
    record.acks = acks;
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 逐个截断点、键绑定、载荷被替换以及有效 CRC 下的非法语义都必须显式失败。
    #[test]
    fn operation_rejects_truncation_cross_key_and_semantic_corruption() {
        let key = "/op/a";
        let parent = CommandId(7);
        let mut record = ControlOperation::prepared(
            parent,
            ControlKind::UserMassCancel(UserId(1)),
            &[ShardId(1), ShardId(2)],
        )
        .unwrap();
        let bytes = encode_record(key, &record).unwrap();
        assert_eq!(decode_record(key, parent, &bytes).unwrap(), record);
        for end in 0..bytes.len() {
            assert!(decode_record(key, parent, &bytes[..end]).is_err());
        }
        assert!(decode_record("/op/b", parent, &bytes).is_err());
        assert!(decode_record(key, CommandId(8), &bytes).is_err());
        let mut damaged = bytes;
        damaged[15] ^= 1;
        assert!(decode_record(key, parent, &damaged).is_err());
        record.generation += 1;
        assert!(decode_record(key, parent, &encode_record(key, &record).unwrap()).is_err());
        record.generation = 1;
        record.targets.insert(ShardId(1), CommandId(0));
        assert!(decode_record(key, parent, &encode_record(key, &record).unwrap()).is_err());
    }

    /// 索引和范围状态的损坏不能被解释为空索引或开放准入。
    #[test]
    fn pending_and_scope_reject_malformed_records() {
        let key = "/pending";
        let mut bytes = b"CIX1".to_vec();
        bytes.extend_from_slice(&42_u128.to_be_bytes());
        bytes.extend_from_slice(&42_u128.to_be_bytes());
        assert!(decode_index(key, &framed(key, bytes)).is_err());
        let scope = Scope {
            epoch: 9,
            killed: true,
            fence: Some(CommandId(23)),
        };
        let mut bytes = scope.encode("/user/a");
        assert!(Scope::decode("/user/b", Some(&bytes)).is_err());
        bytes.pop();
        assert!(Scope::decode("/user/a", Some(&bytes)).is_err());
    }
}
