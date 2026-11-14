//! 独立于分片序号的市场行情源与逐订单视图。
use matching_domain::{MarketId, OrderId, Price, Qty, Side, TradeEvent};
use std::collections::BTreeMap;

/// 市场行情源载荷；是否已提交由外层持久边界决定。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourcePayload {
    /// 新增可见挂单。
    OrderAdded {
        /// 订单身份。
        order_id: OrderId,
        /// 订单方向。
        side: Side,
        /// 挂单价格。
        price: Price,
        /// 可见数量。
        visible: Qty,
    },
    /// 减少可见数量后的剩余值。
    OrderReduced {
        /// 订单身份。
        order_id: OrderId,
        /// 减少后可见数量。
        visible: Qty,
    },
    /// 移除可见挂单。
    OrderRemoved {
        /// 订单身份。
        order_id: OrderId,
    },
    /// 真实成交，不产生逐订单消息。
    TradeOccurred(TradeEvent),
    /// 不改变逐订单视图的管理边界。
    Boundary,
}
/// 单市场顺序排列的行情源事件。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceEvent {
    /// 市场身份。
    pub market_id: MarketId,
    /// 市场内连续源序号。
    pub sequence: u64,
    /// 原序载荷。
    pub payload: SourcePayload,
}
impl SourceEvent {
    /// 编码单条带版本、固定字段和有界长度的行情源事件。
    pub fn encode(&self, limit: usize) -> Result<Vec<u8>, MboError> {
        use matching_domain::snapshot_wire::SnapshotEncoder;
        if self.sequence == 0 {
            return Err(MboError::InvalidSource);
        }
        let mut w = SnapshotEncoder::new(limit);
        let result = (|| {
            w.raw_bytes(b"MSE1")?;
            w.u32(self.market_id.0)?;
            w.u64(self.sequence)?;
            match &self.payload {
                SourcePayload::OrderAdded {
                    order_id,
                    side,
                    price,
                    visible,
                } => {
                    w.u8(1)?;
                    w.u128(order_id.0)?;
                    w.u8(match side {
                        Side::Buy => 0,
                        Side::Sell => 1,
                    })?;
                    w.i64(price.get())?;
                    w.u64(visible.get())?;
                }
                SourcePayload::OrderReduced { order_id, visible } => {
                    w.u8(2)?;
                    w.u128(order_id.0)?;
                    w.u64(visible.get())?;
                }
                SourcePayload::OrderRemoved { order_id } => {
                    w.u8(3)?;
                    w.u128(order_id.0)?;
                }
                SourcePayload::TradeOccurred(trade) => {
                    w.u8(4)?;
                    w.u128(trade.trade_id.0)?;
                    w.u128(trade.maker_order_id.0)?;
                    w.u128(trade.taker_order_id.0)?;
                    w.i64(trade.price.get())?;
                    w.u64(trade.qty.get())?;
                    w.u8(match trade.maker_side {
                        Side::Buy => 0,
                        Side::Sell => 1,
                    })?;
                }
                SourcePayload::Boundary => w.u8(5)?,
            }
            Ok::<_, matching_domain::snapshot_wire::SnapshotWireError>(())
        })();
        result.map_err(|_| MboError::InvalidSource)?;
        Ok(w.finish())
    }
    /// 读取并严格校验一条完整的行情源事件。
    pub fn decode(bytes: &[u8], limit: usize) -> Result<Self, MboError> {
        use matching_domain::snapshot_wire::{SnapshotDecoder, SnapshotWireError};
        let mut r = SnapshotDecoder::new(bytes, limit).map_err(|_| MboError::InvalidSource)?;
        let parsed = (|| {
            if r.u32()? != u32::from_be_bytes(*b"MSE1") {
                return Err(SnapshotWireError);
            }
            let market_id = MarketId(r.u32()?);
            let sequence = r.u64()?;
            if sequence == 0 {
                return Err(SnapshotWireError);
            }
            let side = |tag| match tag {
                0 => Ok(Side::Buy),
                1 => Ok(Side::Sell),
                _ => Err(SnapshotWireError),
            };
            let payload = match r.u8()? {
                1 => SourcePayload::OrderAdded {
                    order_id: OrderId(r.u128()?),
                    side: side(r.u8()?)?,
                    price: Price::try_new(r.i64()?).map_err(|_| SnapshotWireError)?,
                    visible: Qty::try_new(r.u64()?).map_err(|_| SnapshotWireError)?,
                },
                2 => SourcePayload::OrderReduced {
                    order_id: OrderId(r.u128()?),
                    visible: Qty::try_new(r.u64()?).map_err(|_| SnapshotWireError)?,
                },
                3 => SourcePayload::OrderRemoved {
                    order_id: OrderId(r.u128()?),
                },
                4 => SourcePayload::TradeOccurred(TradeEvent {
                    trade_id: matching_domain::TradeId(r.u128()?),
                    maker_order_id: OrderId(r.u128()?),
                    taker_order_id: OrderId(r.u128()?),
                    price: Price::try_new(r.i64()?).map_err(|_| SnapshotWireError)?,
                    qty: Qty::try_new(r.u64()?).map_err(|_| SnapshotWireError)?,
                    maker_side: side(r.u8()?)?,
                }),
                5 => SourcePayload::Boundary,
                _ => return Err(SnapshotWireError),
            };
            r.finish()?;
            Ok(Self {
                market_id,
                sequence,
                payload,
            })
        })();
        parsed.map_err(|_| MboError::InvalidSource)
    }
}
/// 单市场源序号候选；调用方负责与持久提交原子关联。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketSource {
    /// 被跟踪的市场。
    market: MarketId,
    /// 已在此候选分配的最后源序号。
    last: u64,
}

/// 分片的多市场行情候选；安装与持久边界由调用方负责。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketDataState {
    /// 已按业务顺序处理的最后分片变更序号；候选尚不代表持久提交。
    last_applied_shard_seq: u64,
    /// 每个市场独立的序号与订单视图。
    markets: BTreeMap<MarketId, MarketProjector>,
}
impl MarketDataState {
    /// 从没有行情历史的旧快照构造显式边界与当前可见簿；结果仍须随下一命令提交。
    pub(crate) fn bootstrap_legacy(
        books: &BTreeMap<MarketId, matching_orderbook::ProductionOrderBook>,
        last_applied_shard_seq: u64,
    ) -> Result<(Self, BTreeMap<MarketId, Vec<SourceEvent>>), MboError> {
        let mut state = Self::new(books.keys().copied())?;
        state.last_applied_shard_seq = last_applied_shard_seq;
        let mut output = BTreeMap::new();
        for (market, book) in books {
            let projector = state.markets.get_mut(market).ok_or(MboError::SourceGap)?;
            let mut changes = vec![SourcePayload::Boundary];
            for side in [Side::Buy, Side::Sell] {
                for node in book.visible_orders(side) {
                    changes.push(SourcePayload::OrderAdded {
                        order_id: node.original_order().order_id,
                        side,
                        price: node.original_order().price,
                        visible: Qty::try_new(node.visible_remaining())
                            .map_err(|_| MboError::InvalidOrder)?,
                    });
                }
            }
            let events = projector.source.append(&changes)?;
            for event in &events {
                projector.visible.apply(event)?;
            }
            output.insert(*market, events);
        }
        Ok((state, output))
    }
    /// 按明确的市场集合建立空候选，拒绝重复身份。
    pub fn new(markets: impl IntoIterator<Item = MarketId>) -> Result<Self, MboError> {
        let mut result = BTreeMap::new();
        for market in markets {
            if result
                .insert(market, MarketProjector::new(market))
                .is_some()
            {
                return Err(MboError::InvalidOrder);
            }
        }
        Ok(Self {
            last_applied_shard_seq: 0,
            markets: result,
        })
    }
    /// 返回候选已处理的最后分片变更序号。
    pub fn last_applied_shard_seq(&self) -> u64 {
        self.last_applied_shard_seq
    }
    /// 返回指定市场最后分配的源序号。
    pub fn market_event_seq(&self, market: MarketId) -> Option<u64> {
        self.markets
            .get(&market)
            .map(|projector| projector.source.last)
    }
    /// 获取单市场逐订单视图副本；候选本身不证明已经持久化。
    pub(crate) fn builder(&self, market: MarketId) -> Option<MboBuilder> {
        self.markets
            .get(&market)
            .map(|projector| projector.visible.clone())
    }
    /// 返回每个市场的源序号，供匹配快照独立保存和恢复校验。
    pub fn market_watermarks(&self) -> BTreeMap<MarketId, u64> {
        self.markets
            .iter()
            .map(|(market, projector)| (*market, projector.source.last))
            .collect()
    }
    /// 按真实价档链表顺序校验市场、订单详情及同价先后关系。
    pub(crate) fn validate_books(
        &self,
        books: &BTreeMap<MarketId, matching_orderbook::ProductionOrderBook>,
    ) -> Result<(), MboError> {
        if self.markets.keys().ne(books.keys()) {
            return Err(MboError::InvalidCheckpoint);
        }
        for (market, projector) in &self.markets {
            let book = books.get(market).ok_or(MboError::InvalidCheckpoint)?;
            for side in [Side::Buy, Side::Sell] {
                let mut expected: Vec<_> = projector
                    .visible
                    .orders
                    .iter()
                    .filter(|(_, (order_side, _, _, _))| *order_side == side)
                    .map(|(id, (_, price, qty, arrival))| (*id, *price, *qty, *arrival))
                    .collect();
                expected.sort_by(|a, b| {
                    let price_order = if side == Side::Buy {
                        b.1.cmp(&a.1)
                    } else {
                        a.1.cmp(&b.1)
                    };
                    price_order.then_with(|| a.3.cmp(&b.3))
                });
                let actual = book.visible_orders(side);
                if actual.len() != expected.len()
                    || actual
                        .iter()
                        .zip(&expected)
                        .any(|(node, (id, price, qty, _))| {
                            node.original_order().order_id != *id
                                || node.original_order().price != *price
                                || node.visible_remaining() != qty.get()
                        })
                {
                    return Err(MboError::InvalidCheckpoint);
                }
            }
        }
        Ok(())
    }
    /// 编码整个分片的有界候选；调用方必须另行完成持久提交。
    pub fn encode_checkpoint(&self, limit: usize) -> Result<Vec<u8>, MboError> {
        use matching_domain::snapshot_wire::SnapshotEncoder;
        use sha2::{Digest, Sha256};
        let mut w = SnapshotEncoder::new(limit);
        w.raw_bytes(b"MDS1")
            .map_err(|_| MboError::InvalidCheckpoint)?;
        w.u64(self.last_applied_shard_seq)
            .map_err(|_| MboError::InvalidCheckpoint)?;
        w.u32(u32::try_from(self.markets.len()).map_err(|_| MboError::InvalidCheckpoint)?)
            .map_err(|_| MboError::InvalidCheckpoint)?;
        for (market, projector) in &self.markets {
            w.u32(market.0).map_err(|_| MboError::InvalidCheckpoint)?;
            w.bytes(&projector.encode_checkpoint(limit)?)
                .map_err(|_| MboError::InvalidCheckpoint)?;
        }
        let body = w.finish();
        let digest = Sha256::digest(&body);
        let mut result = SnapshotEncoder::new(limit);
        result
            .raw_bytes(&body)
            .map_err(|_| MboError::InvalidCheckpoint)?;
        result
            .raw_bytes(&digest)
            .map_err(|_| MboError::InvalidCheckpoint)?;
        Ok(result.finish())
    }
    /// 验证分片和各市场的外部可信持久前缀；调用方负责证明这些前缀已提交。
    pub fn decode_checkpoint(
        bytes: &[u8],
        limit: usize,
        durable_shard_seq: u64,
        durable: &BTreeMap<MarketId, u64>,
    ) -> Result<Self, MboError> {
        use matching_domain::snapshot_wire::SnapshotDecoder;
        use sha2::{Digest, Sha256};
        let body_len = bytes
            .len()
            .checked_sub(32)
            .ok_or(MboError::InvalidCheckpoint)?;
        if bytes.len() > limit
            || Sha256::digest(&bytes[..body_len]).as_slice() != &bytes[body_len..]
        {
            return Err(MboError::InvalidCheckpoint);
        }
        let mut r = SnapshotDecoder::new(&bytes[..body_len], limit)
            .map_err(|_| MboError::InvalidCheckpoint)?;
        if r.u32().map_err(|_| MboError::InvalidCheckpoint)? != u32::from_be_bytes(*b"MDS1") {
            return Err(MboError::InvalidCheckpoint);
        }
        let last_applied_shard_seq = r.u64().map_err(|_| MboError::InvalidCheckpoint)?;
        if last_applied_shard_seq > durable_shard_seq {
            return Err(MboError::InvalidCheckpoint);
        }
        let count = usize::try_from(r.u32().map_err(|_| MboError::InvalidCheckpoint)?)
            .map_err(|_| MboError::InvalidCheckpoint)?;
        if count != durable.len() || count > r.remaining() / 8 {
            return Err(MboError::InvalidCheckpoint);
        }
        let mut markets = BTreeMap::new();
        let mut previous = None;
        for _ in 0..count {
            let market = MarketId(r.u32().map_err(|_| MboError::InvalidCheckpoint)?);
            if previous.is_some_and(|old| old >= market) {
                return Err(MboError::InvalidCheckpoint);
            }
            let watermark = *durable.get(&market).ok_or(MboError::InvalidCheckpoint)?;
            let checkpoint = r.bytes().map_err(|_| MboError::InvalidCheckpoint)?;
            markets.insert(
                market,
                MarketProjector::decode_checkpoint(checkpoint, limit, market, watermark)?,
            );
            previous = Some(market);
        }
        r.finish().map_err(|_| MboError::InvalidCheckpoint)?;
        Ok(Self {
            last_applied_shard_seq,
            markets,
        })
    }
    /// 对涉及的每个市场预演同一执行结果；任一失败则不安装任何市场。
    pub fn apply(
        &mut self,
        execution: &crate::FormalExecution,
    ) -> Result<BTreeMap<MarketId, Vec<SourceEvent>>, MboError> {
        if !execution.deduplicated
            && self.last_applied_shard_seq.checked_add(1) != Some(execution.seq)
        {
            return Err(MboError::SourceGap);
        }
        let mut candidate = self.clone();
        let mut output = BTreeMap::new();
        let targets: std::collections::BTreeSet<_> = execution
            .events
            .iter()
            .filter_map(|event| event.market_id)
            .collect();
        if targets.is_empty()
            && (!execution.market_data_child_opens.is_empty()
                || !execution.market_data_open_details.is_empty())
        {
            return Err(MboError::InvalidOrder);
        }
        for market in targets {
            let projector = candidate
                .markets
                .get_mut(&market)
                .ok_or(MboError::SourceGap)?;
            let events = projector.apply_formal_execution(execution, |_, _| None)?;
            if !events.is_empty() {
                output.insert(market, events);
            }
        }
        if execution.deduplicated
            && (!execution.events.is_empty()
                || !execution.market_data_child_opens.is_empty()
                || !execution.market_data_open_details.is_empty())
        {
            return Err(MboError::InvalidOrder);
        }
        if !execution.deduplicated {
            candidate.last_applied_shard_seq = execution.seq;
        }
        *self = candidate;
        Ok(output)
    }
}

/// 将原序业务事件投影到隔离市场源候选；不负责持久提交。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketProjector {
    /// 同一市场的源序号候选。
    source: MarketSource,
    /// 用于检查可见订单变动及保持前态的构建器。
    visible: MboBuilder,
}
impl MarketProjector {
    /// 创建空市场投影候选。
    pub fn new(market: MarketId) -> Self {
        Self {
            source: MarketSource::new(market),
            visible: MboBuilder::new(market),
        }
    }
    /// 投影单市场正式执行结果；调用方须先证明结果属于已提交前缀。
    /// 分片控制事件按市场过滤；其他结果必须只包含当前市场。
    pub fn apply_formal_execution<F>(
        &mut self,
        execution: &crate::FormalExecution,
        mut opened: F,
    ) -> Result<Vec<SourceEvent>, MboError>
    where
        F: FnMut(usize, OrderId) -> Option<(Side, Price, Qty)>,
    {
        if execution.deduplicated {
            if !execution.events.is_empty()
                || !execution.market_data_child_opens.is_empty()
                || !execution.market_data_open_details.is_empty()
            {
                return Err(MboError::InvalidOrder);
            }
            return Ok(Vec::new());
        }
        let mut payloads = Vec::new();
        let mut original_indices = Vec::new();
        payloads
            .try_reserve_exact(execution.events.len())
            .map_err(|_| MboError::Capacity)?;
        original_indices
            .try_reserve_exact(execution.events.len())
            .map_err(|_| MboError::Capacity)?;
        let shard_control = matches!(execution.outcome, crate::FormalOutcome::ShardControl);
        if shard_control && !execution.market_data_child_opens.is_empty() {
            return Err(MboError::InvalidOrder);
        }
        let mut details = BTreeMap::new();
        for item in &execution.market_data_open_details {
            let index = usize::try_from(item.event_index).map_err(|_| MboError::InvalidOrder)?;
            if !matches!(execution.events.get(index).map(|event| &event.payload), Some(matching_domain::EngineEvent::OrderOpened { order_id }) if *order_id == item.order_id)
                || details
                    .insert(index, (item.side, item.price, item.visible))
                    .is_some()
            {
                return Err(MboError::InvalidOrder);
            }
        }
        let identity = execution
            .events
            .first()
            .map(|event| (event.cluster_id, event.shard_id, event.engine_time));
        for (index, event) in execution.events.iter().enumerate() {
            if event.shard_seq != execution.seq
                || event.command_id != execution.command_id
                || usize::try_from(event.event_index).ok() != Some(index)
                || Some((event.cluster_id, event.shard_id, event.engine_time)) != identity
            {
                return Err(MboError::SourceGap);
            }
            if event.market_id != Some(self.source.market) {
                if shard_control {
                    continue;
                }
                return Err(MboError::SourceGap);
            }
            payloads.push(event.payload.clone());
            original_indices.push(index);
        }
        self.apply_engine_events_with_child_opens(
            &payloads,
            &execution.market_data_child_opens,
            |index, id| {
                details
                    .get(&original_indices[index])
                    .copied()
                    .or_else(|| opened(original_indices[index], id))
            },
        )
    }
    /// 查询当前候选中的订单方向、价格和可见量。
    pub fn visible_order(&self, id: OrderId) -> Option<(Side, Price, Qty)> {
        self.visible
            .orders
            .get(&id)
            .map(|(side, price, qty, _)| (*side, *price, *qty))
    }
    /// 编码投影候选；结果须由调用方持久化才能作为恢复依据。
    pub fn encode_checkpoint(&self, limit: usize) -> Result<Vec<u8>, MboError> {
        if self.source.market != self.visible.market || self.source.last != self.visible.source {
            return Err(MboError::InvalidCheckpoint);
        }
        self.visible.encode_checkpoint(limit)
    }
    /// 使用可信持久源前缀恢复候选；调用方负责验证该前缀的来源。
    pub fn decode_checkpoint(
        bytes: &[u8],
        limit: usize,
        market: MarketId,
        durable_source: u64,
    ) -> Result<Self, MboError> {
        let visible = MboBuilder::decode_checkpoint(bytes, limit, market, durable_source)?;
        Ok(Self {
            source: MarketSource {
                market,
                last: visible.source,
            },
            visible,
        })
    }
    /// 将止损候选提供的中间挂单轨迹插入对应事件位置，再以同一原子候选投影。
    pub fn apply_engine_events_with_child_opens<F>(
        &mut self,
        events: &[matching_domain::EngineEvent],
        opens: &[matching_orderbook::production_orderbook::TriggeredChildOpen],
        mut opened: F,
    ) -> Result<Vec<SourceEvent>, MboError>
    where
        F: FnMut(usize, OrderId) -> Option<(Side, Price, Qty)>,
    {
        use matching_domain::EngineEvent;
        let capacity = events
            .len()
            .checked_add(opens.len())
            .ok_or(MboError::Overflow)?;
        let mut expanded = Vec::new();
        let mut original_indices = Vec::new();
        expanded
            .try_reserve_exact(capacity)
            .map_err(|_| MboError::Capacity)?;
        original_indices
            .try_reserve_exact(capacity)
            .map_err(|_| MboError::Capacity)?;
        let mut details = BTreeMap::new();
        let mut position = 0usize;
        let mut child = None;
        for boundary in 0..=events.len() {
            while position < opens.len() && opens[position].after_event_count == boundary {
                let item = &opens[position];
                if child != Some(item.order_id) {
                    return Err(MboError::InvalidOrder);
                }
                details.insert(expanded.len(), (item.side, item.price, item.visible));
                expanded.push(EngineEvent::OrderOpened {
                    order_id: item.order_id,
                });
                original_indices.push(None);
                child = None;
                position = position.checked_add(1).ok_or(MboError::Overflow)?;
            }
            if let Some(event) = events.get(boundary) {
                if let EngineEvent::StopTriggered { order_id, .. } = event {
                    child = Some(*order_id);
                }
                expanded.push(event.clone());
                original_indices.push(Some(boundary));
            }
        }
        if position != opens.len() {
            return Err(MboError::InvalidOrder);
        }
        self.apply_engine_events(&expanded, |index, id| {
            details
                .get(&index)
                .copied()
                .or_else(|| original_indices[index].and_then(|original| opened(original, id)))
        })
    }
    /// 按事件原序投影，在全部检查通过后安装候选；挂单详情必须对应事件位置并来自可信候选。
    pub fn apply_engine_events<F>(
        &mut self,
        events: &[matching_domain::EngineEvent],
        mut opened: F,
    ) -> Result<Vec<SourceEvent>, MboError>
    where
        F: FnMut(usize, OrderId) -> Option<(Side, Price, Qty)>,
    {
        use matching_domain::EngineEvent;
        let mut candidate = self.clone();
        let mut exhausted = BTreeMap::new();
        let mut output = Vec::new();
        for (event_index, event) in events.iter().enumerate() {
            let mut changes = Vec::new();
            match event {
                EngineEvent::Trade(trade) => {
                    let &(side, price, visible, _) = candidate
                        .visible
                        .orders
                        .get(&trade.maker_order_id)
                        .ok_or(MboError::InvalidOrder)?;
                    if side != trade.maker_side || price != trade.price || trade.qty > visible {
                        return Err(MboError::InvalidOrder);
                    }
                    changes.push(SourcePayload::TradeOccurred(trade.clone()));
                    let remaining = visible.get() - trade.qty.get();
                    if remaining == 0 {
                        exhausted.insert(trade.maker_order_id, (side, price));
                        changes.push(SourcePayload::OrderRemoved {
                            order_id: trade.maker_order_id,
                        });
                    } else {
                        changes.push(SourcePayload::OrderReduced {
                            order_id: trade.maker_order_id,
                            visible: Qty::try_new(remaining).map_err(|_| MboError::InvalidOrder)?,
                        });
                    }
                }
                EngineEvent::OrderOpened { order_id } => {
                    let (side, price, visible) =
                        opened(event_index, *order_id).ok_or(MboError::InvalidOrder)?;
                    changes.push(SourcePayload::OrderAdded {
                        order_id: *order_id,
                        side,
                        price,
                        visible,
                    });
                }
                EngineEvent::IcebergRefreshed { order_id, visible } => {
                    let (side, price) = exhausted.remove(order_id).ok_or(MboError::InvalidOrder)?;
                    changes.push(SourcePayload::OrderAdded {
                        order_id: *order_id,
                        side,
                        price,
                        visible: *visible,
                    });
                }
                EngineEvent::OrderAmended {
                    order_id,
                    old_price,
                    new_price,
                    old_quantity,
                    new_quantity,
                    priority_retained,
                } if candidate.visible.orders.contains_key(order_id) => {
                    let &(old_side, price, before, _) = candidate
                        .visible
                        .orders
                        .get(order_id)
                        .ok_or(MboError::InvalidOrder)?;
                    let (side, after_price, after) =
                        opened(event_index, *order_id).ok_or(MboError::InvalidOrder)?;
                    let matching_domain::AmendedQuantity::BaseRemaining(total) = new_quantity
                    else {
                        return Err(MboError::InvalidOrder);
                    };
                    let matching_domain::AmendedQuantity::BaseRemaining(old_total) = old_quantity
                    else {
                        return Err(MboError::InvalidOrder);
                    };
                    if !priority_retained
                        || *old_price != Some(price)
                        || *new_price != Some(price)
                        || side != old_side
                        || after_price != price
                        || after > before
                        || after > *total
                        || before > *old_total
                        || total > old_total
                    {
                        return Err(MboError::InvalidOrder);
                    }
                    if after < before {
                        changes.push(SourcePayload::OrderReduced {
                            order_id: *order_id,
                            visible: after,
                        });
                    }
                }
                EngineEvent::OrderFilled { order_id }
                    if candidate.visible.orders.contains_key(order_id) =>
                {
                    return Err(MboError::InvalidOrder);
                }
                EngineEvent::OrderCanceled { order_id, .. }
                | EngineEvent::OrderExpired { order_id, .. }
                    if candidate.visible.orders.contains_key(order_id) =>
                {
                    changes.push(SourcePayload::OrderRemoved {
                        order_id: *order_id,
                    });
                }
                _ => {}
            }
            for source in candidate.source.append(&changes)? {
                candidate.visible.apply(&source)?;
                output.push(source);
            }
        }
        *self = candidate;
        Ok(output)
    }
}
impl MarketSource {
    /// 创建从零开始的市场源候选。
    pub fn new(market: MarketId) -> Self {
        Self { market, last: 0 }
    }
    /// 返回候选中的末尾源序号，不单独证明持久提交。
    pub fn last_sequence(&self) -> u64 {
        self.last
    }
    /// 分配一个有序批次；分配或数值失败不推进候选。
    pub fn append(&mut self, payloads: &[SourcePayload]) -> Result<Vec<SourceEvent>, MboError> {
        let count = u64::try_from(payloads.len()).map_err(|_| MboError::Overflow)?;
        let end = self.last.checked_add(count).ok_or(MboError::Overflow)?;
        let mut events = Vec::new();
        events
            .try_reserve_exact(payloads.len())
            .map_err(|_| MboError::Capacity)?;
        let mut seq = self.last;
        for payload in payloads {
            seq = seq.checked_add(1).ok_or(MboError::Overflow)?;
            events.push(SourceEvent {
                market_id: self.market,
                sequence: seq,
                payload: payload.clone(),
            });
        }
        self.last = end;
        Ok(events)
    }
}
/// 单个公开逐订单消息。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MboMessage {
    /// 此消息所属的市场。
    pub market_id: MarketId,
    /// 独立逐订单序号。
    pub feed_seq: u64,
    /// 产生消息的源水位。
    pub source_seq: u64,
    /// 逐订单变化。
    pub change: MboChange,
}
/// 逐订单视图的变更。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MboChange {
    /// 新增挂单。
    Added {
        /// 订单身份。
        order_id: OrderId,
        /// 订单方向。
        side: Side,
        /// 挂单价格。
        price: Price,
        /// 可见数量。
        visible: Qty,
    },
    /// 可见数量减少。
    Reduced {
        /// 订单身份。
        order_id: OrderId,
        /// 减少后可见数量。
        visible: Qty,
    },
    /// 删除挂单。
    Removed {
        /// 订单身份。
        order_id: OrderId,
    },
}
/// 恢复或消费时检测到不连续或无效状态。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MboError {
    /// 市场或源序号不匹配。
    SourceGap,
    /// 订单变化与当前状态不一致。
    InvalidOrder,
    /// 序号、事件位置或可见数量聚合溢出。
    Overflow,
    /// 检查点格式、容量、摘要或可信水位不匹配。
    InvalidCheckpoint,
    /// 无法分配有界批次的存储空间。
    Capacity,
    /// 行情源编码、字段或容量无效。
    InvalidSource,
}
/// 逐订单构建器的完整进程内状态。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MboBuilder {
    /// 单一市场的身份。
    market: MarketId,
    /// 已消费的最后市场源序号。
    source: u64,
    /// 已产生的最后逐订单消息序号。
    feed: u64,
    /// 可见挂单及其挂入源序号。
    orders: BTreeMap<OrderId, (Side, Price, Qty, u64)>,
}
impl MboBuilder {
    /// 创建空市场的构建器。
    pub fn new(market: MarketId) -> Self {
        Self {
            market,
            source: 0,
            feed: 0,
            orders: BTreeMap::new(),
        }
    }
    /// 返回状态副本；它本身并不证明已经持久化。
    pub fn checkpoint(&self) -> Self {
        self.clone()
    }
    /// 编码有界检查点；调用方负责原子持久化和绑定可信源水位。
    pub fn encode_checkpoint(&self, limit: usize) -> Result<Vec<u8>, MboError> {
        use matching_domain::snapshot_wire::SnapshotEncoder;
        use sha2::{Digest, Sha256};
        let mut w = SnapshotEncoder::new(limit);
        let result = (|| {
            w.raw_bytes(b"MBO1")?;
            w.u32(self.market.0)?;
            w.u64(self.source)?;
            w.u64(self.feed)?;
            w.u32(
                u32::try_from(self.orders.len())
                    .map_err(|_| matching_domain::snapshot_wire::SnapshotWireError)?,
            )?;
            for (id, (side, price, qty, arrival)) in &self.orders {
                w.u128(id.0)?;
                w.u8(match side {
                    Side::Buy => 0,
                    Side::Sell => 1,
                })?;
                w.i64(price.get())?;
                w.u64(qty.get())?;
                w.u64(*arrival)?;
            }
            Ok::<_, matching_domain::snapshot_wire::SnapshotWireError>(())
        })();
        result.map_err(|_| MboError::InvalidCheckpoint)?;
        let body = w.finish();
        let digest = Sha256::digest(&body);
        let mut final_writer = SnapshotEncoder::new(limit);
        final_writer
            .raw_bytes(&body)
            .map_err(|_| MboError::InvalidCheckpoint)?;
        final_writer
            .raw_bytes(&digest)
            .map_err(|_| MboError::InvalidCheckpoint)?;
        Ok(final_writer.finish())
    }
    /// 在调用方提供可信已持久源前缀后恢复检查点；该参数本身不是持久性证明。
    pub fn decode_checkpoint(
        bytes: &[u8],
        limit: usize,
        market: MarketId,
        durable_source: u64,
    ) -> Result<Self, MboError> {
        use matching_domain::snapshot_wire::SnapshotDecoder;
        use sha2::{Digest, Sha256};
        let body_len = bytes
            .len()
            .checked_sub(32)
            .ok_or(MboError::InvalidCheckpoint)?;
        if bytes.len() > limit
            || Sha256::digest(&bytes[..body_len]).as_slice() != &bytes[body_len..]
        {
            return Err(MboError::InvalidCheckpoint);
        }
        let mut r = SnapshotDecoder::new(&bytes[..body_len], limit)
            .map_err(|_| MboError::InvalidCheckpoint)?;
        let parsed = (|| {
            if r.u32()? != u32::from_be_bytes(*b"MBO1") || r.u32()? != market.0 {
                return Err(matching_domain::snapshot_wire::SnapshotWireError);
            }
            let source = r.u64()?;
            let feed = r.u64()?;
            if source > durable_source || feed > source {
                return Err(matching_domain::snapshot_wire::SnapshotWireError);
            }
            let count = r.u32()?;
            if u64::from(count) > feed {
                return Err(matching_domain::snapshot_wire::SnapshotWireError);
            }
            let mut orders = BTreeMap::new();
            let mut arrivals = std::collections::BTreeSet::new();
            for _ in 0..count {
                let id = OrderId(r.u128()?);
                let side = match r.u8()? {
                    0 => Side::Buy,
                    1 => Side::Sell,
                    _ => return Err(matching_domain::snapshot_wire::SnapshotWireError),
                };
                let price = Price::try_new(r.i64()?)
                    .map_err(|_| matching_domain::snapshot_wire::SnapshotWireError)?;
                let qty = Qty::try_new(r.u64()?)
                    .map_err(|_| matching_domain::snapshot_wire::SnapshotWireError)?;
                let arrival = r.u64()?;
                if arrival == 0
                    || arrival > source
                    || !arrivals.insert(arrival)
                    || orders.insert(id, (side, price, qty, arrival)).is_some()
                {
                    return Err(matching_domain::snapshot_wire::SnapshotWireError);
                }
            }
            r.finish()?;
            Ok(Self {
                market,
                source,
                feed,
                orders,
            })
        })();
        parsed.map_err(|_| MboError::InvalidCheckpoint)
    }
    /// 以挂入源序返回活动订单，供恢复同价先后顺序使用。
    pub fn orders_in_insertion_order(&self) -> Vec<OrderId> {
        let mut entries: Vec<_> = self.orders.iter().map(|(id, item)| (item.3, *id)).collect();
        entries.sort_unstable();
        entries.into_iter().map(|(_, id)| id).collect()
    }
    /// 以价格优先顺序聚合可见量；数量溢出显式失败。
    pub fn visible_depth(&self, side: Side) -> Result<Vec<(Price, u64)>, MboError> {
        let mut levels = BTreeMap::<Price, u64>::new();
        for (order_side, price, qty, _) in self.orders.values() {
            if *order_side == side {
                let total = levels.entry(*price).or_default();
                *total = total.checked_add(qty.get()).ok_or(MboError::Overflow)?;
            }
        }
        let mut result: Vec<_> = levels.into_iter().collect();
        if side == Side::Buy {
            result.reverse();
        }
        Ok(result)
    }
    /// 计算包含源水位、输出水位和全部可见订单的确定性摘要。
    pub fn digest(&self) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut hash = Sha256::new();
        hash.update(b"tide-mbo-state-v1\0");
        hash.update(self.market.0.to_be_bytes());
        hash.update(self.source.to_be_bytes());
        hash.update(self.feed.to_be_bytes());
        for (id, (side, price, qty, arrival)) in &self.orders {
            hash.update(id.0.to_be_bytes());
            hash.update([match side {
                Side::Buy => 0,
                Side::Sell => 1,
            }]);
            hash.update(price.get().to_be_bytes());
            hash.update(qty.get().to_be_bytes());
            hash.update(arrival.to_be_bytes());
        }
        hash.finalize().into()
    }
    /// 顺序处理一条行情源；失败不改变状态。
    pub fn apply(&mut self, event: &SourceEvent) -> Result<Option<MboMessage>, MboError> {
        if event.market_id != self.market || self.source.checked_add(1) != Some(event.sequence) {
            return Err(MboError::SourceGap);
        }
        let change = match &event.payload {
            SourcePayload::OrderAdded {
                order_id,
                side,
                price,
                visible,
            } => {
                if self.orders.contains_key(order_id) {
                    return Err(MboError::InvalidOrder);
                }
                Some(MboChange::Added {
                    order_id: *order_id,
                    side: *side,
                    price: *price,
                    visible: *visible,
                })
            }
            SourcePayload::OrderReduced { order_id, visible } => {
                let old = self.orders.get(order_id).ok_or(MboError::InvalidOrder)?;
                if *visible >= old.2 {
                    return Err(MboError::InvalidOrder);
                }
                Some(MboChange::Reduced {
                    order_id: *order_id,
                    visible: *visible,
                })
            }
            SourcePayload::OrderRemoved { order_id } => {
                if !self.orders.contains_key(order_id) {
                    return Err(MboError::InvalidOrder);
                }
                Some(MboChange::Removed {
                    order_id: *order_id,
                })
            }
            SourcePayload::TradeOccurred(_) | SourcePayload::Boundary => None,
        };
        let feed = if change.is_some() {
            self.feed.checked_add(1).ok_or(MboError::Overflow)?
        } else {
            self.feed
        };
        match &change {
            Some(MboChange::Added {
                order_id,
                side,
                price,
                visible,
            }) => {
                self.orders
                    .insert(*order_id, (*side, *price, *visible, event.sequence));
            }
            Some(MboChange::Reduced { order_id, visible }) => {
                self.orders.get_mut(order_id).expect("已校验订单").2 = *visible;
            }
            Some(MboChange::Removed { order_id }) => {
                self.orders.remove(order_id);
            }
            None => {}
        }
        self.source = event.sequence;
        self.feed = feed;
        Ok(change.map(|change| MboMessage {
            market_id: self.market,
            feed_seq: feed,
            source_seq: event.sequence,
            change,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_sequence_overflow_preserves_candidate() {
        let mut source = MarketSource {
            market: MarketId(1),
            last: u64::MAX,
        };
        let before = source.clone();
        assert_eq!(
            source.append(&[SourcePayload::Boundary]),
            Err(MboError::Overflow)
        );
        assert_eq!(source, before);
    }

    #[test]
    fn feed_sequence_overflow_does_not_consume_source() {
        let mut builder = MboBuilder {
            market: MarketId(1),
            source: 0,
            feed: u64::MAX,
            orders: BTreeMap::new(),
        };
        let before = builder.clone();
        let event = SourceEvent {
            market_id: MarketId(1),
            sequence: 1,
            payload: SourcePayload::OrderAdded {
                order_id: OrderId(1),
                side: Side::Buy,
                price: Price::try_new(1).unwrap(),
                visible: Qty::try_new(1).unwrap(),
            },
        };
        assert_eq!(builder.apply(&event), Err(MboError::Overflow));
        assert_eq!(builder, before);
    }

    #[test]
    fn candidate_snapshot_must_match_book_visible_fifo() {
        use matching_domain::{EngineEvent, LimitOrder, StpKey, StpMode, UserId};
        use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
        let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(10).unwrap());
        let mut data = MarketDataState::new([MarketId(1)]).unwrap();
        for id in [2_u128, 1] {
            book.rest(LimitOrder {
                order_id: OrderId(id),
                user_id: UserId(id as u64),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Buy,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(1).unwrap(),
            })
            .unwrap();
            data.markets
                .get_mut(&MarketId(1))
                .unwrap()
                .apply_engine_events(
                    &[EngineEvent::OrderOpened {
                        order_id: OrderId(id),
                    }],
                    |_, _| {
                        Some((
                            Side::Buy,
                            Price::try_new(10).unwrap(),
                            Qty::try_new(1).unwrap(),
                        ))
                    },
                )
                .unwrap();
        }
        assert!(
            data.validate_books(&BTreeMap::from([(MarketId(1), book.clone())]))
                .is_ok()
        );
        let mut wrong_fifo = MarketDataState::new([MarketId(1)]).unwrap();
        for id in [1_u128, 2] {
            wrong_fifo
                .markets
                .get_mut(&MarketId(1))
                .unwrap()
                .apply_engine_events(
                    &[EngineEvent::OrderOpened {
                        order_id: OrderId(id),
                    }],
                    |_, _| {
                        Some((
                            Side::Buy,
                            Price::try_new(10).unwrap(),
                            Qty::try_new(1).unwrap(),
                        ))
                    },
                )
                .unwrap();
        }
        assert!(
            wrong_fifo
                .validate_books(&BTreeMap::from([(MarketId(1), book.clone())]))
                .is_err()
        );
        book.cancel(OrderId(2)).unwrap();
        assert!(
            data.validate_books(&BTreeMap::from([(MarketId(1), book)]))
                .is_err()
        );
    }

    #[test]
    fn legacy_book_bootstrap_emits_boundary_and_real_fifo() {
        use matching_domain::{LimitOrder, StpKey, StpMode, UserId};
        use matching_orderbook::{ProductionOrderBook, QueuePriorityPolicy};
        let mut book = ProductionOrderBook::new(QueuePriorityPolicy::try_new(10).unwrap());
        for id in [2_u128, 1] {
            book.rest(LimitOrder {
                order_id: OrderId(id),
                user_id: UserId(id as u64),
                stp_key: StpKey::new(0),
                stp_mode: StpMode::None,
                side: Side::Buy,
                price: Price::try_new(10).unwrap(),
                qty: Qty::try_new(1).unwrap(),
            })
            .unwrap();
        }
        let books = BTreeMap::from([(MarketId(1), book)]);
        let (state, events) = MarketDataState::bootstrap_legacy(&books, 5).unwrap();
        assert_eq!(state.last_applied_shard_seq(), 5);
        assert_eq!(state.market_event_seq(MarketId(1)), Some(3));
        assert!(matches!(
            events[&MarketId(1)][0].payload,
            SourcePayload::Boundary
        ));
        assert!(matches!(
            events[&MarketId(1)][1].payload,
            SourcePayload::OrderAdded {
                order_id: OrderId(2),
                ..
            }
        ));
        assert!(matches!(
            events[&MarketId(1)][2].payload,
            SourcePayload::OrderAdded {
                order_id: OrderId(1),
                ..
            }
        ));
        state.validate_books(&books).unwrap();
    }
}
