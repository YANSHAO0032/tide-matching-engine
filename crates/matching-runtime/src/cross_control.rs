//! 跨分片控制操作的身份、持久状态转换与准入契约。
use matching_domain::{ClusterId, CommandId, RejectReason, ShardId, UserId};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, io};

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// 一个父命令绑定的控制意图；重新提交相同父命令时范围与策略必须完全一致。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlKind {
    /// 撤销指定用户在所有目标分片的订单。
    UserMassCancel(UserId),
    /// 撤销所有目标分片的全部订单。
    GlobalMassCancel,
    /// 设置用户开关，可在启用时撤单。
    UserKill {
        /// 开关所属用户。
        user: UserId,
        /// 启用或清除开关。
        active: bool,
        /// 在同一次启用中撤单。
        cancel_all: bool,
    },
    /// 设置全局开关，可在启用时撤单。
    GlobalKill {
        /// 启用或清除开关。
        active: bool,
        /// 在同一次启用中撤单。
        cancel_all: bool,
    },
}

impl ControlKind {
    /// 按固定协议派生分片命令身份。主节点纪元、重试次数和操作代数不参与派生。
    pub fn derive(self, parent: CommandId, shard: ShardId) -> CommandId {
        let mut hash = Sha256::new();
        hash.update(b"tide/cross-control/v1");
        hash.update(parent.get().to_be_bytes());
        hash.update(shard.get().to_be_bytes());
        match self {
            Self::UserMassCancel(user) => {
                hash.update([1]);
                hash.update(user.get().to_be_bytes());
            }
            Self::GlobalMassCancel => hash.update([2]),
            Self::UserKill {
                user,
                active,
                cancel_all,
            } => {
                hash.update([3]);
                hash.update(user.get().to_be_bytes());
                hash.update([u8::from(active), u8::from(cancel_all)]);
            }
            Self::GlobalKill { active, cancel_all } => {
                hash.update([4, u8::from(active), u8::from(cancel_all)])
            }
        }
        let mut id = [0; 16];
        id.copy_from_slice(&hash.finalize()[..16]);
        CommandId(u128::from_be_bytes(id))
    }
}

/// 持久操作记录的当前阶段。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlPhase {
    /// 已固定意图及分片拓扑。
    Prepared,
    /// 已独占范围并关闭受保护的准入。
    Fencing,
    /// 准备向各分片派发，尚未授权任何在途请求。
    Fanout,
    /// 已授权外部派发，开始收集各分片的持久回执。
    AwaitingAcks,
    /// 已确认全部分片及撤单总数，临时栅栏仍有效。
    Committed,
    /// 已原子发布新纪元与开关状态，并解除该范围的临时栅栏。
    Released,
    /// 已从待办索引移除，可按父命令重取终态结果。
    Done,
    /// 在尚未授权任何派发时安全终止。
    Aborted,
}

/// 分片返回的首次持久执行结果。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControlAck {
    /// 首次提交的分片序号。
    pub shard_seq: u64,
    /// 此分片实际撤销的订单数。
    pub canceled: u64,
}

/// 同一父命令的持久协调记录。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ControlOperation {
    /// 唯一绑定该意图的外部命令。
    pub parent: CommandId,
    /// 固定的范围与开关策略。
    pub kind: ControlKind,
    /// 已持久化的协调阶段。
    pub phase: ControlPhase,
    /// 初次登记时固化的分片及各自的派生命令。
    pub targets: BTreeMap<ShardId, CommandId>,
    /// 已取得首次执行结果的分片。
    pub acks: BTreeMap<ShardId, ControlAck>,
    /// 进入栅栏时由存储原子分配的范围纪元。
    pub epoch: u64,
    /// 初值为一的操作代数，每次阶段变更或首次登记某分片回执时加一。
    pub generation: u64,
}

/// 持久状态机允许的一次变更。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlUpdate {
    /// 进入下一个正常阶段。
    Next,
    /// 登记目标分片及其首次持久回执。
    Ack(ShardId, ControlAck),
    /// 仅在准备、持栅栏或准备派发阶段中止。一旦授权派发，即可能已有在途请求。
    Abort,
}

impl ControlOperation {
    /// 校验意图并建立按分片排序的固定目标集。
    pub fn prepared(parent: CommandId, kind: ControlKind, targets: &[ShardId]) -> io::Result<Self> {
        if targets.is_empty()
            || targets.len() > 4096
            || matches!(
                kind,
                ControlKind::UserKill {
                    active: false,
                    cancel_all: true,
                    ..
                } | ControlKind::GlobalKill {
                    active: false,
                    cancel_all: true
                }
            )
        {
            return Err(invalid("控制范围或开关策略无效"));
        }
        let derived: BTreeMap<_, _> = targets
            .iter()
            .map(|&shard| (shard, kind.derive(parent, shard)))
            .collect();
        if derived.len() != targets.len() {
            return Err(invalid("目标分片重复"));
        }
        Ok(Self {
            parent,
            kind,
            phase: ControlPhase::Prepared,
            targets: derived,
            acks: BTreeMap::new(),
            epoch: 0,
            generation: 1,
        })
    }

    /// 计算单步候选，调用方须以原记录 CAS 持久化。进入栅栏时在同一事务分配范围纪元。
    pub fn transition(&self, update: ControlUpdate) -> io::Result<Self> {
        use ControlPhase::*;
        let mut next = self.clone();
        next.phase = match (self.phase, update) {
            (Prepared, ControlUpdate::Next) => Fencing,
            (Fencing, ControlUpdate::Next) if self.epoch != 0 => Fanout,
            (Fanout, ControlUpdate::Next) => AwaitingAcks,
            (AwaitingAcks, ControlUpdate::Ack(shard, ack))
                if self.targets.contains_key(&shard) && ack.shard_seq != 0 =>
            {
                if let Some(previous) = self.acks.get(&shard) {
                    if previous != &ack {
                        return Err(invalid("同一分片的首次回执冲突"));
                    }
                    return Ok(next);
                }
                next.acks.insert(shard, ack);
                AwaitingAcks
            }
            (AwaitingAcks, ControlUpdate::Next) if self.acks.len() == self.targets.len() => {
                self.total_canceled()?;
                Committed
            }
            (Committed, ControlUpdate::Next) => Released,
            (Released, ControlUpdate::Next) => Done,
            (Prepared | Fencing | Fanout, ControlUpdate::Abort) => Aborted,
            _ => return Err(invalid("当前控制阶段不允许此变更")),
        };
        next.generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid("操作代数耗尽"))?;
        Ok(next)
    }

    /// 对已登记回执的实际撤单数求和。全体回执齐全才得到最终总数，溢出时显式失败。
    pub fn total_canceled(&self) -> io::Result<u64> {
        self.acks.values().try_fold(0u64, |n, a| {
            n.checked_add(a.canceled)
                .ok_or_else(|| invalid("撤单结果超出范围"))
        })
    }
}

/// 生产者在入队前读取的线性一致准入视图，单写工作线程只核对随消息传入的纪元。
pub trait ControlAdmission: Send + Sync {
    /// 此准入权威绑定的业务集群。
    fn cluster_id(&self) -> ClusterId;
    /// 控制存储、业务集群和键空间的稳定绑定。
    fn namespace(&self) -> String;
    /// 同时返回已发布的全局纪元、用户纪元及当前拒绝原因。读取失败时入口必须关闭。
    fn check(&self, user: UserId) -> io::Result<(u64, u64, Option<RejectReason>)>;
}

/// 持有协调者租约的操作存储；所有写入须在事务内验证所有者。
pub trait ControlAuthority: ControlAdmission {
    /// 持久登记意图；重复父命令核对已有记录后返回。
    fn prepare(
        &self,
        parent: CommandId,
        kind: ControlKind,
        targets: &[ShardId],
    ) -> io::Result<ControlOperation>;
    /// 线性一致读取父命令记录。
    fn load(&self, parent: CommandId) -> io::Result<ControlOperation>;
    /// 获取仍需推进的全部持久操作。
    fn pending(&self) -> io::Result<Vec<CommandId>>;
    /// 原子比较所有者、旧记录及范围状态，提交一次变更。
    fn advance(
        &self,
        previous: &ControlOperation,
        update: ControlUpdate,
    ) -> io::Result<ControlOperation>;
}

mod coordinator;
pub use coordinator::{ControlTelemetry, CrossShardCoordinator};
