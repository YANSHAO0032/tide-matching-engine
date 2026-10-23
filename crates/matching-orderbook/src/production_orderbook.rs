//! 单市场订单簿状态、构造与基础查询；各订单策略在独立子模块中实现。

use crate::{
    OrderArena, OrderIndex, OrderNode, PriceLevel, QueuePriorityAllocator, QueuePriorityPolicy,
};
use matching_domain::{OrderId, Price, QueuePriority, Side};
use std::collections::{BTreeMap, HashMap};

mod iceberg;
mod limit_execution;
mod market_by_base;
mod market_by_quote;
mod matching;
mod resting;

/// 指明已预检的余量将追加到既有档位还是新建档位。
///
/// 该枚举只保存提交前后必须保持一致的值，不保留对容器的借用；
/// 因而预检完成后仍可执行挂单状态变更，再通过失败即停止的断言检查
/// 提交时的所有者状态没有发生意外漂移。
#[derive(Debug, Clone, Copy)]
enum RestTargetPlan {
    Existing {
        /// 提交前既有价档的先进先出尾节点。
        tail: OrderIndex,
        /// 提交前既有价档中的节点数量。
        count_before: u32,
        /// 提交前既有价档的可见数量聚合值。
        total_before: u64,
        /// 提交后既有价档应具有的节点数量。
        count_after: u32,
        /// 提交后既有价档应具有的可见数量聚合值。
        total_after: u64,
    },
    New {
        /// 提交后新建价档应具有的节点数量。
        count_after: u32,
        /// 提交后新建价档应具有的可见数量聚合值。
        total_after: u64,
    },
}

/// `preflight_rest_common` 产生的只读挂单提交计划。
///
/// 它记录身份、计划分配的优先级及目标档位的聚合快照。
/// 公开 `rest` 另行施加严格不交叉；完整限价单 GTC 则在
/// 挂单已被预检后复用本计划，保证余量的可预期拒绝发生在首笔成交前。
#[derive(Debug, Clone, Copy)]
struct RestPlan {
    /// 即将挂入订单簿的订单标识符。
    order_id: OrderId,
    /// 挂单所在的买卖方向。
    side: Side,
    /// 挂单所属的价格档位价格。
    price: Price,
    /// 已预留给本次实际挂单的队列优先级。
    priority: QueuePriority,
    /// 既有或新建价格档位的预检聚合计划。
    target: RestTargetPlan,
}

/// 连续成交的只读预测结果。
///
/// `trade_count` 决定调用方至少要提供多少 TradeId，`remaining` 是
/// 本次连续撮合结束后吃单方的预测余量。它不拥有节点或可变引用，
/// 也不改变新到订单、订单簿或优先级分配器。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FillPlan {
    /// 预期产生的成交笔数。
    trade_count: usize,
    /// 连续撮合结束后吃单方的基础资产余量。
    remaining: u64,
}

/// 单市场生产订单簿的查询、挂单、取消和撮合入口。
///
/// 价格顺序由双侧有序索引保存，同价先进先出由 PriceLevel 链表维护；
/// OrderId 索引只承担定位，不决定业务顺序。当前支持已确定应挂单的
/// 限价单 GTC 余量、已知标识符的取消、完整限价单 GTC、市价订单，以及
/// 限价单 IOC/FOK/PostOnly 与冰山。实际挂单及冰山补量时分配优先级；不包含完整资金和命令协议。
#[derive(Debug)]
pub struct ProductionOrderBook {
    /// 买侧价格档位；合法非空条目中的节点必须具有相同价格和买入方向。
    bids: BTreeMap<Price, PriceLevel>,
    /// 卖侧价格档位；合法非空条目中的节点必须具有相同价格和卖出方向。
    asks: BTreeMap<Price, PriceLevel>,
    /// 当前活动 OrderId 到 Arena 槽位的定位索引，必须与节点身份一致。
    orders: HashMap<OrderId, OrderIndex>,
    /// 节点的唯一存储；价格档位和标识符索引通过 OrderIndex 引用它。
    arena: OrderArena,
    /// 本市场独占的优先级状态；只读查询不消耗序号。
    queue_priority_allocator: QueuePriorityAllocator,
}

impl ProductionOrderBook {
    /// 使用已验证的策略创建空簿，市场内优先级从零开始。
    pub fn new(policy: QueuePriorityPolicy) -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            orders: HashMap::new(),
            arena: OrderArena::new(),
            queue_priority_allocator: QueuePriorityAllocator::new(policy),
        }
    }

    /// 查询指定簿侧的最优价格，空侧返回 None；方向不是新到订单方向。
    ///
    /// 契约要求买入取最高买价、卖出取最低卖价，不分配优先级或缓存价格。
    pub fn best_price(&self, side: Side) -> Option<Price> {
        match side {
            Side::Buy => self.bids.last_key_value().map(|(price, _)| *price),
            Side::Sell => self.asks.first_key_value().map(|(price, _)| *price),
        }
    }

    /// 经单键标识符查找借用 Arena 节点；未知标识符返回 None。
    ///
    /// # 不变量断言
    ///
    /// 索引命中但槽位不存在或节点身份不匹配时，按内部不变量损坏停止，
    /// 不将损坏静默解释成订单不存在。此查询不遍历或校验整条价档链。
    pub fn order(&self, order_id: OrderId) -> Option<&OrderNode> {
        let index = *self.orders.get(&order_id)?;

        let node = self.arena.get(index).unwrap_or_else(|| {
            panic!(
                "ProductionOrderBook invariant violation: \
                 order index references missing arena slot: \
                 order_id={order_id:?}, index={index:?}"
            )
        });
        assert_eq!(
            node.original_order().order_id,
            order_id,
            "ProductionOrderBook invariant violation: \
             order index points to different order: \
             requested={order_id:?}, actual={:?}, index={index:?}",
            node.original_order().order_id,
        );
        Some(node)
    }

    /// 读取下一次分配将使用的优先级，不推进分配器。
    pub fn next_queue_priority(&self) -> u128 {
        self.queue_priority_allocator.next()
    }
}

#[cfg(test)]
mod tests;
