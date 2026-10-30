//! 参考簿的价格与先进先出选择、逐笔成交规划和执行。

use super::ReferenceOrderBook;
use crate::{IncomingOrder, ReferenceOrder};
use matching_domain::{LimitOrder, OrderId, Price, Qty, RejectReason, Side, TradeEvent, TradeId};

impl ReferenceOrderBook {
    /// 返回对 `incoming` 价格可成交的最优挂单。
    ///
    /// 选择遵循对手方价格优先和同价先进先出，并忽略已经耗尽的运行态。
    pub fn best_crossing_maker(&self, incoming: &LimitOrder) -> Option<&ReferenceOrder> {
        self.best_crossing_maker_index(incoming)
            .map(|index| &self.orders[index])
    }

    /// 查找最佳可成交挂单在源 `Vec` 中的索引。
    ///
    /// 该索引只在当前不可变借用期间有效，供后续规划和一次性执行定位同一挂单。
    pub(crate) fn best_crossing_maker_index(&self, incoming: &LimitOrder) -> Option<usize> {
        let opposite_side = match incoming.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };
        let maker = self.orders(opposite_side).into_iter().find(|maker| {
            if maker.remaining() == 0 {
                return false;
            }
            let maker_price = maker.original_order().price;

            match incoming.side {
                Side::Buy => incoming.price >= maker_price,
                Side::Sell => incoming.price <= maker_price,
            }
        })?;
        self.orders
            .iter()
            .position(|source| std::ptr::eq(source, maker))
    }

    /// 返回下一笔成交的挂单与数量，但不修改双方运行态。
    ///
    /// 与内部 `plan_next_fill` 共用价格/先进先出选择逻辑进行只读预览；
    /// 返回的引用在订单簿发生可变操作前有效。
    pub fn next_fill_plan(
        &self,
        incoming: &IncomingOrder,
    ) -> Result<Option<(&ReferenceOrder, Qty)>, RejectReason> {
        let Some((maker_index, fill_qty)) = self.plan_next_fill(incoming)? else {
            return Ok(None);
        };
        Ok(Some((&self.orders[maker_index], fill_qty)))
    }

    /// 执行至多一笔 `next`-成交，不生成事件，也不将新到订单剩余量挂簿。
    ///
    /// 先规划并预检双方扣减，再修改运行态。
    /// 返回挂单身份、挂单价格和成交量。
    pub fn execute_next_fill(
        &mut self,
        incoming: &mut IncomingOrder,
    ) -> Result<Option<(OrderId, Price, Qty)>, RejectReason> {
        if incoming.original_order().stp_mode != matching_domain::StpMode::None {
            return Err(RejectReason::InvalidOrderCombination);
        }
        if self.has_iceberg_maker(incoming.original_order().side) {
            return self
                .execute_slice_fills(incoming, true)
                .map(|fills| fills.into_iter().next());
        }
        let Some((maker_index, fill_qty)) = self.plan_next_fill(incoming)? else {
            return Ok(None);
        };
        let lots = fill_qty.get();
        let maker = &self.orders[maker_index];

        maker
            .remaining()
            .checked_sub(lots)
            .ok_or(RejectReason::ArithmeticOverflow)?;
        incoming
            .remaining()
            .checked_sub(lots)
            .ok_or(RejectReason::ArithmeticOverflow)?;

        let maker_id = maker.original_order().order_id;
        let maker_price = maker.original_order().price;

        self.orders[maker_index].apply_fill(fill_qty)?;
        incoming.apply_fill(fill_qty)?;

        if self.orders[maker_index].remaining() == 0 {
            self.orders.remove(maker_index);
            self.reservations.remove(&maker_id);
        }

        Ok(Some((maker_id, maker_price, fill_qty)))
    }

    /// 持续执行单笔成交，直到新到订单耗尽或不存在可成交挂单。
    ///
    /// 结果按实际成交顺序排列，价格取各挂单的价格。
    /// 冰山挂单存在时，先对整条命令模拟补量/优先级再提交。
    /// 此既有接口仅投影成交；不挂单新到订单，富事件接口保留生命周期事件。
    pub fn execute_fills(
        &mut self,
        incoming: &mut IncomingOrder,
    ) -> Result<Vec<(OrderId, Price, Qty)>, RejectReason> {
        if incoming.original_order().stp_mode != matching_domain::StpMode::None {
            return Err(RejectReason::InvalidOrderCombination);
        }
        if self.has_iceberg_maker(incoming.original_order().side) {
            return self.execute_slice_fills(incoming, false);
        }
        let mut fills = Vec::new();
        while let Some(fill) = self.execute_next_fill(incoming)? {
            fills.push(fill)
        }
        Ok(fills)
    }

    /// 执行至多一笔真实成交，并转换为 TradeEvent。
    ///
    /// TradeId 由调用方提供；无可成交挂单时返回 None。
    /// 此既有接口仅投影 TradeEvent；不写 WAL，不自动挂单新到订单。
    pub fn execute_next_fill_trade(
        &mut self,
        incoming: &mut IncomingOrder,
        trade_id: TradeId,
    ) -> Result<Option<TradeEvent>, RejectReason> {
        let maker_side = match incoming.original_order().side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        };

        let taker_order_id = incoming.original_order().order_id;

        let Some((maker_order_id, price, qty)) = self.execute_next_fill(incoming)? else {
            return Ok(None);
        };

        Ok(Some(TradeEvent {
            trade_id,
            maker_order_id,
            taker_order_id,
            price,
            qty,
            maker_side,
        }))
    }

    /// 规划下一笔成交，返回源 Vec 索引和成交数量。
    ///
    /// 不修改订单簿，索引仅供内部使用。
    fn plan_next_fill(
        &self,
        incoming: &IncomingOrder,
    ) -> Result<Option<(usize, Qty)>, RejectReason> {
        if incoming.remaining() == 0 {
            return Ok(None);
        }
        let Some(maker_index) = self.best_crossing_maker_index(incoming.original_order()) else {
            return Ok(None);
        };
        // 计算双方当前最多能够成交的数量
        let maker_remaining = self.orders[maker_index].remaining();
        let fill_lots = incoming.remaining().min(maker_remaining);

        let fill_qty = Qty::try_new(fill_lots)?;
        Ok(Some((maker_index, fill_qty)))
    }
}
