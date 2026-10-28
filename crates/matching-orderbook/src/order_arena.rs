use crate::{OrderIndex, OrderNode};
use matching_domain::RejectReason;
#[cfg(test)]
use std::cell::Cell;

/// 索引式订单 Arena。
///
/// OrderIndex 表示槽位位置，继续追加不会改变已有索引。
/// 空槽由 None 表示，不使用特殊索引值。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrderArena {
    /// 按 `OrderIndex` 定位的稳定槽位；删除留下 `None`，不会压缩或重排。
    slots: Vec<Option<OrderNode>>,
    /// 已释放空槽的后进先出栈；每个元素必须有效且恰好对应一个 `None` 槽位。
    free: Vec<OrderIndex>,
    #[cfg(test)]
    /// 仅测试中的实际槽位访问计数，不是业务状态，也不参与重放。
    slot_accesses: Cell<usize>,
}

impl OrderArena {
    /// 冻结提交屏障需要的全部槽位与空闲栈；测试访问计数不属于业务状态。
    pub(crate) fn state_snapshot(&self) -> (Vec<Option<OrderNode>>, Vec<OrderIndex>) {
        (self.slots.clone(), self.free.clone())
    }

    /// 创建没有槽位和空闲链表条目的空 Arena。
    pub fn new() -> Self {
        Self::default()
    }

    /// 插入订单节点，返回其槽位索引。
    ///
    /// 优先按照后进先出顺序复用空闲链表中的空槽；
    /// 没有空槽时，进行带溢出检查的索引转换后追加。
    ///
    /// 复用前必须验证索引有效且槽位为空，
    /// 内部不变量被破坏时直接触发不变量断言，不覆盖现存节点。
    ///
    /// 删除后旧逻辑索引失效；复用后，相同的 OrderIndex
    /// 数值会指向新节点。当前尚无代次校验。
    pub fn insert(&mut self, node: OrderNode) -> Result<OrderIndex, RejectReason> {
        // 后进先出：优先复用最后释放的槽位。
        if let Some(&index) = self.free.last() {
            let position = usize::try_from(index.get()).expect("OrderIndex must fit usize");
            // 验证空闲尾部指向的槽位。
            #[cfg(test)]
            self.record_slot_access();
            // 必须先验证，不能先 `pop`。
            // 越界或指向已占用槽位时，在修改任何状态前触发不变量断言。
            assert!(
                matches!(self.slots.get(position), Some(None)),
                "OrderArena invariant violation: free index is invalid or occupied"
            );
            // 只有验证成功，才能消费空闲并写入节点。
            self.free.pop();
            // 写入复用槽位。
            #[cfg(test)]
            self.record_slot_access();
            self.slots[position] = Some(node);
            return Ok(index);
        }

        let index = OrderIndex::try_from_usize(self.slots.len())?;
        #[cfg(test)]
        self.record_slot_access();
        self.slots.push(Some(node));
        Ok(index)
    }

    /// 获取指定槽位中的订单节点。
    ///
    /// 索引越界或槽位为空时返回 None；该查询不验证节点链接或其
    /// `PriceLevel` 成员归属。
    pub fn get(&self, index: OrderIndex) -> Option<&OrderNode> {
        let position = usize::try_from(index.get()).ok()?;
        #[cfg(test)]
        self.record_slot_access();
        self.slots.get(position)?.as_ref()
    }

    /// 取走节点并将其槽位登记到空闲链表。
    ///
    /// 仅成功删除时登记一次，不压缩槽位。
    /// 越界、空槽及尚未复用前的重复删除返回 None，
    /// 并保持槽位和空闲不变。
    ///
    /// 调用前必须解除链表引用。若所有者在同一独占状态变更内随后清理
    /// OrderId 映射，则清理完成前不得查询旧映射、复用槽位或暴露中间状态。
    pub fn remove(&mut self, index: OrderIndex) -> Option<OrderNode> {
        let position = usize::try_from(index.get()).ok()?;
        #[cfg(test)]
        self.record_slot_access();
        // 只有成功取出节点才登记空闲。
        let node = self.slots.get_mut(position)?.take()?;

        self.free.push(index);

        Some(node)
    }

    /// 仅供库内部在完成局部不变量预检后维护节点运行态。
    ///
    /// `None` 同时表示越界或已释放槽位；此访问不修复链接，也不验证
    /// 节点属于某个 PriceLevel。
    pub(crate) fn get_mut(&mut self, index: OrderIndex) -> Option<&mut OrderNode> {
        let position = usize::try_from(index.get()).ok()?;
        #[cfg(test)]
        self.record_slot_access();
        self.slots.get_mut(position)?.as_mut()
    }

    /// 记录一次真实槽位读取或写入，仅用于 O(1) 访问路径测试。
    ///
    /// 计数溢出是测试诊断失败，不能改变生产错误语义。
    #[cfg(test)]
    fn record_slot_access(&self) {
        let next = self
            .slot_accesses
            .get()
            .checked_add(1)
            .expect("OrderArena slot access counter overflow");

        self.slot_accesses.set(next);
    }

    /// 只读验证下一次 `insert` 是否可执行。
    ///
    /// 不弹出空闲链表，不扩展槽位，不修改任何 Arena 状态。
    pub(crate) fn preflight_insert(&self) -> Result<(), RejectReason> {
        if let Some(index) = self.free.last().copied() {
            let position = usize::try_from(index.get())
                .expect("OrderIndex must fit usize on supported targets");

            let slot = self.slots.get(position).unwrap_or_else(|| {
                panic!(
                    "OrderArena invariant violation: \
                 free-list tail references missing slot: \
                 index={index:?}"
                )
            });

            assert!(
                slot.is_none(),
                "OrderArena invariant violation: \
             free-list tail references occupied slot: \
             index={index:?}"
            );

            return Ok(());
        }

        // 没有空闲槽位时，下一次 `insert` 必须追加。
        // 这里只验证索引可表示性，不真正扩容。
        OrderIndex::try_from_usize(self.slots.len())?;

        Ok(())
    }
}

#[cfg(test)]
#[path = "order_arena/tests.rs"]
mod tests;
