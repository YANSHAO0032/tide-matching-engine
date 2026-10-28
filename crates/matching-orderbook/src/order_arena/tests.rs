//! Arena 生命周期、空闲链表和常数槽位访问证据。
//!
//! 此模块的模型与快照只用于验证实现，不参与生产状态或重放。

use super::*;
use matching_domain::{LimitOrder, OrderId, Price, Qty, QueuePriority, Side, UserId};
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};

impl OrderArena {
    /// 将测试访问计数归零；不会触碰槽位或空闲链表。
    pub(crate) fn reset_slot_accesses(&self) {
        self.slot_accesses.set(0);
    }

    /// 读取测试访问计数；它不是运行时指标或持久化数据。
    pub(crate) fn slot_accesses(&self) -> usize {
        self.slot_accesses.get()
    }

    /// 复制完整 Arena 内存态，用于测试失败原子性。
    ///
    /// 此辅助函数不模拟快照、WAL 或重放，也不会暴露给生产调用方。
    pub(crate) fn test_snapshot(&self) -> (Vec<Option<OrderNode>>, Vec<OrderIndex>) {
        (self.slots.clone(), self.free.clone())
    }
}

/// 构造具有固定有效价格的最小测试订单。
fn order(id: u128, side: Side, qty: u64) -> LimitOrder {
    LimitOrder {
        order_id: OrderId::new(id),
        user_id: UserId::new(id as u64),
        side,
        price: Price::try_new(100).unwrap(),
        qty: Qty::try_new(qty).unwrap(),
    }
}

/// 将受控测试位置转换为已知可表示的 Arena 槽位。
fn index(position: usize) -> OrderIndex {
    OrderIndex::try_from_usize(position).unwrap()
}

/// 断言一个有效槽位完整保留订单、数量、优先级和未链接状态。
fn assert_node(
    arena: &OrderArena,
    index: OrderIndex,
    expected: &LimitOrder,
    priority: QueuePriority,
) {
    let node = arena.get(index).expect("node must exist");

    assert_eq!(node.original_order(), expected);
    assert_eq!(node.remaining(), expected.qty.get());
    assert_eq!(node.priority(), priority);
    assert_eq!(node.prev(), None);
    assert_eq!(node.next(), None);
}

/// 用于比较槽位内容的完整节点投影，包含所有运行态字段。
type NodeState = (
    LimitOrder,
    u64,
    QueuePriority,
    Option<OrderIndex>,
    Option<OrderIndex>,
);

/// 从节点复制完整测试投影，避免断言遗漏任何字段。
fn node_state(node: &OrderNode) -> NodeState {
    (
        node.original_order().clone(),
        node.remaining(),
        node.priority(),
        node.prev(),
        node.next(),
    )
}

/// 复制槽位的内容投影，用于验证失败不会改变任何槽位。
fn snapshot_slots(arena: &OrderArena) -> Vec<Option<NodeState>> {
    arena
        .slots
        .iter()
        .map(|slot| slot.as_ref().map(node_state))
        .collect()
}

#[path = "tests/lifecycle.rs"]
mod lifecycle;
#[path = "tests/metrics.rs"]
mod metrics;
#[path = "tests/model.rs"]
mod model;
#[path = "tests/preflight.rs"]
mod preflight;
