//! 从有序市场源构造独立的公开价档、最优报价和成交序列。
use crate::{MboBuilder, MboError, MboMessage, SourceEvent, SourcePayload};
use matching_domain::{MarketId, Price, Qty, ShardId, Side, TradeId};
use std::collections::BTreeMap;

/// 单侧价格档和其公开可见总量；零表示删除该价档。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MbpMessage {
    /// 市场身份。
    pub market_id: MarketId,
    /// 此行情流的连续序号。
    pub feed_seq: u64,
    /// 产生本消息的市场源序号，可能因过滤而跳跃。
    pub source_seq: u64,
    /// 价档方向。
    pub side: Side,
    /// 价档价格。
    pub price: Price,
    /// 变化后的可见总量。
    pub visible: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use matching_domain::OrderId;

    #[test]
    fn digest_covers_materialized_depth() {
        let mut builder = FeedBuilder::new(MarketId(1));
        let before = builder.digest();
        builder.bids.push((Price::try_new(1).unwrap(), 1));
        assert_ne!(before, builder.digest());
    }

    #[test]
    fn exhausted_feed_sequences_leave_source_and_all_feeds_unchanged() {
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
        for mbp in [true, false] {
            let mut builder = FeedBuilder::new(MarketId(1));
            if mbp {
                builder.mbp_seq = u64::MAX;
            } else {
                builder.bbo_seq = u64::MAX;
            }
            let before = builder.clone();
            assert_eq!(builder.apply(&event), Err(MboError::Overflow));
            assert_eq!(builder, before);
        }
        let mut builder = FeedBuilder::new(MarketId(1));
        builder.trade_seq = u64::MAX;
        let before = builder.clone();
        let trade = SourceEvent {
            market_id: MarketId(1),
            sequence: 1,
            payload: SourcePayload::TimedTradeOccurred {
                trade: matching_domain::TradeEvent {
                    trade_id: TradeId(1),
                    maker_order_id: OrderId(1),
                    taker_order_id: OrderId(2),
                    price: Price::try_new(1).unwrap(),
                    qty: Qty::try_new(1).unwrap(),
                    maker_side: Side::Buy,
                },
                engine_time: 1,
            },
        };
        assert_eq!(builder.apply(&trade), Err(MboError::Overflow));
        assert_eq!(builder, before);
    }
}

/// 一侧的最优价格和公开可见总量。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BestLevel {
    /// 最优价格。
    pub price: Price,
    /// 该价格的可见总量。
    pub visible: u64,
}

/// 双侧最优报价变化。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BboMessage {
    /// 市场身份。
    pub market_id: MarketId,
    /// 此行情流的连续序号。
    pub feed_seq: u64,
    /// 产生本消息的市场源序号，可能因过滤而跳跃。
    pub source_seq: u64,
    /// 最优买档。
    pub bid: Option<BestLevel>,
    /// 最优卖档。
    pub ask: Option<BestLevel>,
}

/// 不暴露订单或用户身份的公开成交。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicTrade {
    /// 成交身份，可用于去重；不能作为市场内排序依据。
    pub trade_id: TradeId,
    /// 市场身份。
    pub market_id: MarketId,
    /// 成交行情流的连续序号。
    pub feed_seq: u64,
    /// 产生本消息的市场源序号，可能因过滤而跳跃。
    pub source_seq: u64,
    /// 成交价格。
    pub price: Price,
    /// 成交数量。
    pub qty: Qty,
    /// 挂单方向。
    pub maker_side: Side,
    /// 成交的逻辑时间。
    pub engine_time: u64,
}

/// 处理一条源事件后产生的各行情流消息。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FeedOutput {
    /// 逐订单变化。
    pub mbo: Option<MboMessage>,
    /// 可见价档变化。
    pub mbp: Option<MbpMessage>,
    /// 双侧最优报价变化。
    pub bbo: Option<BboMessage>,
    /// 真实成交。
    pub trade: Option<PublicTrade>,
}

/// 各公开行情流在同一构建器切面的序号。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeedSeqSnapshot {
    /// 最后输出的逐订单流序号；尚无输出时为零。
    pub mbo: u64,
    /// 最后输出的价档流序号；尚无输出时为零。
    pub mbp: u64,
    /// 最后输出的最优报价流序号；尚无输出时为零。
    pub bbo: u64,
    /// 最后输出的成交流序号；尚无输出时为零。
    pub trade: u64,
}

/// 供管理层记录的市场迁移边界，不代表迁移已经完成。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MarketMigrationBoundary {
    /// 市场身份。
    pub market_id: MarketId,
    /// 原分片。
    pub old_shard_id: ShardId,
    /// 目标分片。
    pub new_shard_id: ShardId,
    /// 目标路由纪元。
    pub route_epoch: u64,
    /// 最后消费的市场源序号。
    pub last_market_event_seq: u64,
    /// 供诊断的各行情流序号。
    pub last_feed_seq: FeedSeqSnapshot,
}

/// 与价档流水位位于同一切面的公开深度快照。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct L2Snapshot {
    /// 市场身份。
    pub market_id: MarketId,
    /// 获取快照时由路由层确认的纪元。
    pub route_epoch: u64,
    /// 快照包含的最后价档序号。
    pub feed_seq: u64,
    /// 快照包含的最后市场源序号。
    pub source_seq: u64,
    /// 买档，按价格从高到低排列。
    pub bids: Vec<(Price, u64)>,
    /// 卖档，按价格从低到高排列。
    pub asks: Vec<(Price, u64)>,
}

/// 客户端检测到市场不匹配、缺口或非法消息。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResumeError {
    /// 消息市场身份不匹配。
    Market,
    /// 流水位不连续或已耗尽。
    Gap,
    /// 消息与客户端状态矛盾。
    Invalid,
}

/// 单条过滤行情流的客户端去重与缺口检测游标。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeedCursor {
    /// 所属市场。
    market: MarketId,
    /// 最后接受的本流序号。
    sequence: u64,
}

impl FeedCursor {
    /// 从可信的本流水位开始检查。
    pub fn new(market: MarketId, sequence: u64) -> Self {
        Self { market, sequence }
    }
    /// 检查市场和本流序号，重复返回 false，缺口不推进游标。
    pub fn accept(&mut self, market: MarketId, sequence: u64) -> Result<bool, ResumeError> {
        if market != self.market {
            return Err(ResumeError::Market);
        }
        if sequence <= self.sequence {
            return Ok(false);
        }
        if self.sequence.checked_add(1) != Some(sequence) {
            return Err(ResumeError::Gap);
        }
        self.sequence = sequence;
        Ok(true)
    }
}

/// 使用价档流水位去重并恢复公开深度的客户端状态。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct L2Client {
    /// 当前已接受的价档切面。
    snapshot: L2Snapshot,
    /// 是否已发现必须重新取快照的缺口。
    gap: bool,
}

/// 逐订单状态及其同一切面的源和行情流水位。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct L3Snapshot {
    /// 同一切面的逐订单状态和水位。
    builder: MboBuilder,
    /// 获取切面时的路由纪元。
    route_epoch: u64,
}

impl L3Snapshot {
    /// 返回完整逐订单视图，按方向、价格优先及同价挂入顺序排列。
    pub fn orders(&self) -> Vec<crate::MboOrder> {
        self.builder.visible_orders()
    }
    /// 返回快照所属的路由纪元。
    pub fn route_epoch(&self) -> u64 {
        self.route_epoch
    }
    /// 依次返回市场源水位和逐订单流水位。
    pub fn watermarks(&self) -> (u64, u64) {
        self.builder.watermarks()
    }
    /// 编码完整逐订单切面，供传输使用。
    pub fn encode(&self, limit: usize) -> Result<Vec<u8>, MboError> {
        use matching_domain::snapshot_wire::SnapshotEncoder;
        use sha2::{Digest, Sha256};
        let inner = self.builder.encode_checkpoint(limit)?;
        let mut w = SnapshotEncoder::new(limit);
        w.raw_bytes(b"L3S1")
            .and_then(|_| w.u64(self.route_epoch))
            .and_then(|_| w.bytes(&inner))
            .map_err(|_| MboError::InvalidCheckpoint)?;
        let body = w.finish();
        let hash = Sha256::digest(&body);
        let mut w = SnapshotEncoder::new(limit);
        w.raw_bytes(&body)
            .and_then(|_| w.raw_bytes(&hash))
            .map_err(|_| MboError::InvalidCheckpoint)?;
        Ok(w.finish())
    }
    /// 校验并解码接收到的逐订单切面；摘要用于发现意外损坏，不验证发送方身份。
    pub fn decode(bytes: &[u8], limit: usize, market: MarketId) -> Result<Self, MboError> {
        use matching_domain::snapshot_wire::SnapshotDecoder;
        use sha2::{Digest, Sha256};
        let n = bytes
            .len()
            .checked_sub(32)
            .ok_or(MboError::InvalidCheckpoint)?;
        if bytes.len() > limit || Sha256::digest(&bytes[..n]).as_slice() != &bytes[n..] {
            return Err(MboError::InvalidCheckpoint);
        }
        let mut r =
            SnapshotDecoder::new(&bytes[..n], limit).map_err(|_| MboError::InvalidCheckpoint)?;
        if r.u32().map_err(|_| MboError::InvalidCheckpoint)? != u32::from_be_bytes(*b"L3S1") {
            return Err(MboError::InvalidCheckpoint);
        }
        let route_epoch = r.u64().map_err(|_| MboError::InvalidCheckpoint)?;
        let builder = MboBuilder::decode_checkpoint(
            r.bytes().map_err(|_| MboError::InvalidCheckpoint)?,
            limit,
            market,
            u64::MAX,
        )?;
        r.finish().map_err(|_| MboError::InvalidCheckpoint)?;
        builder.visible_depth(Side::Buy)?;
        builder.visible_depth(Side::Sell)?;
        Ok(Self {
            builder,
            route_epoch,
        })
    }
}

/// 使用逐订单行情流水位恢复的客户端状态。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct L3Client {
    /// 当前已接受的逐订单视图。
    builder: MboBuilder,
    /// 是否已发现必须重新取快照的缺口。
    gap: bool,
}

impl L3Client {
    /// 返回当前逐订单视图；调用方须先检查视图有效性。
    pub fn orders(&self) -> Vec<crate::MboOrder> {
        self.builder.visible_orders()
    }
    /// 从可信的一致切面开始恢复。
    pub fn from_snapshot(snapshot: L3Snapshot) -> Self {
        Self {
            builder: snapshot.builder,
            gap: false,
        }
    }
    /// 未发现序号缺口时返回 true；它不证明输入来自可信发送方。
    pub fn is_valid(&self) -> bool {
        !self.gap
    }
    /// 丢弃重复消息；出现缺口后必须重新获取快照。
    pub fn apply(&mut self, message: &MboMessage) -> Result<bool, ResumeError> {
        if self.gap {
            return Err(ResumeError::Gap);
        }
        let (_, seq) = self.builder.watermarks();
        if message.market_id != self.builder.market_id() {
            return Err(ResumeError::Market);
        }
        if message.feed_seq <= seq {
            return Ok(false);
        }
        if seq.checked_add(1) != Some(message.feed_seq) {
            self.gap = true;
            return Err(ResumeError::Gap);
        }
        self.builder
            .apply_feed_message(message)
            .map_err(|_| ResumeError::Invalid)?;
        Ok(true)
    }
    /// 从逐订单状态聚合价格优先深度；调用方须先检查视图有效性。
    pub fn depth(&self, side: Side) -> Result<Vec<(Price, u64)>, MboError> {
        self.builder.visible_depth(side)
    }
}

impl L2Client {
    /// 从可信的一致切面开始恢复，并校验价档排序、正数量及水位关系。
    pub fn from_snapshot(snapshot: L2Snapshot) -> Result<Self, ResumeError> {
        let valid = |levels: &[(Price, u64)], side| {
            levels.iter().all(|(_, qty)| *qty > 0)
                && levels.windows(2).all(|pair| match side {
                    Side::Buy => pair[0].0 > pair[1].0,
                    Side::Sell => pair[0].0 < pair[1].0,
                })
        };
        if snapshot.feed_seq > snapshot.source_seq
            || snapshot
                .bids
                .len()
                .checked_add(snapshot.asks.len())
                .and_then(|count| u64::try_from(count).ok())
                .is_none_or(|count| count > snapshot.feed_seq)
            || !valid(&snapshot.bids, Side::Buy)
            || !valid(&snapshot.asks, Side::Sell)
        {
            return Err(ResumeError::Invalid);
        }
        Ok(Self {
            snapshot,
            gap: false,
        })
    }

    /// 未发现序号缺口时返回 true；它不证明输入来自可信发送方。
    pub fn is_valid(&self) -> bool {
        !self.gap
    }
    /// 丢弃已包含的增量；出现缺口后必须重新获取快照。
    pub fn apply(&mut self, message: &MbpMessage) -> Result<bool, ResumeError> {
        if self.gap {
            return Err(ResumeError::Gap);
        }
        if message.market_id != self.snapshot.market_id {
            return Err(ResumeError::Market);
        }
        if message.feed_seq <= self.snapshot.feed_seq {
            return Ok(false);
        }
        if self.snapshot.feed_seq.checked_add(1) != Some(message.feed_seq) {
            self.gap = true;
            return Err(ResumeError::Gap);
        }
        if message.source_seq <= self.snapshot.source_seq {
            return Err(ResumeError::Invalid);
        }
        let levels = match message.side {
            Side::Buy => &mut self.snapshot.bids,
            Side::Sell => &mut self.snapshot.asks,
        };
        if let Some(index) = levels.iter().position(|(price, _)| *price == message.price) {
            if message.visible == 0 {
                levels.remove(index);
            } else {
                levels[index].1 = message.visible;
            }
        } else if message.visible == 0 {
            return Err(ResumeError::Invalid);
        } else {
            levels.push((message.price, message.visible));
            levels.sort_unstable_by_key(|(price, _)| *price);
            if message.side == Side::Buy {
                levels.reverse();
            }
        }
        self.snapshot.feed_seq = message.feed_seq;
        self.snapshot.source_seq = message.source_seq;
        Ok(true)
    }

    /// 返回价格优先排列的价档；调用方须先检查视图有效性。
    pub fn depth(&self, side: Side) -> Vec<(Price, u64)> {
        match side {
            Side::Buy => &self.snapshot.bids,
            Side::Sell => &self.snapshot.asks,
        }
        .clone()
    }
}

/// 单市场的进程内行情构建器；调用方负责只消费已提交的源。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeedBuilder {
    /// 单一市场。
    market: MarketId,
    /// 顺序验证和逐订单状态。
    mbo: MboBuilder,
    /// 价格优先排列的买档。
    bids: Vec<(Price, u64)>,
    /// 价格优先排列的卖档。
    asks: Vec<(Price, u64)>,
    /// 价档流水位。
    mbp_seq: u64,
    /// 最优报价流水位。
    bbo_seq: u64,
    /// 成交流水位。
    trade_seq: u64,
}

impl FeedBuilder {
    /// 读取构建器所属市场，不修改行情状态。
    pub fn market_id(&self) -> MarketId {
        self.market
    }

    /// 读取同一构建器切面的源序号及各独立行情流水位。
    pub fn watermarks(&self) -> (u64, FeedSeqSnapshot) {
        let (source, mbo) = self.mbo.watermarks();
        (
            source,
            FeedSeqSnapshot {
                mbo,
                mbp: self.mbp_seq,
                bbo: self.bbo_seq,
                trade: self.trade_seq,
            },
        )
    }

    /// 将检查点写入独占目录中的新文件并同步。
    /// 调用方须保证状态来自已提交源，并在写入成功后登记可信身份和水位。
    pub fn write_checkpoint_file(
        &self,
        directory: impl AsRef<std::path::Path>,
        id: u64,
        limit: usize,
    ) -> std::io::Result<()> {
        let bytes = self.encode_checkpoint(limit).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "行情检查点编码失败")
        })?;
        matching_persistence::write_snapshot_file(directory, id, self.mbo.watermarks().0, &bytes)
    }

    /// 按调用方可信登记的身份和源水位读取并校验检查点。
    pub fn read_checkpoint_file(
        directory: impl AsRef<std::path::Path>,
        id: u64,
        source: u64,
        limit: usize,
        market: MarketId,
    ) -> std::io::Result<Self> {
        let path = directory.as_ref().join(format!("snapshot-{id}.bin"));
        let (actual_id, actual_source, bytes) = matching_persistence::read_snapshot(path)?;
        if actual_id != id || actual_source != source {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "行情检查点身份不匹配",
            ));
        }
        let result = Self::decode_checkpoint(&bytes, limit, market, source)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "行情检查点无效"))?;
        if result.mbo.watermarks().0 != source {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "行情检查点水位不匹配",
            ));
        }
        Ok(result)
    }
    /// 从可信检查点和完整已提交源后缀恢复，并核对最终水位及摘要。
    /// 持久水位、源后缀及预期摘要的可信性由调用方保证。
    pub fn recover(
        bytes: &[u8],
        limit: usize,
        market: MarketId,
        durable_source: u64,
        suffix: &[SourceEvent],
        expected_digest: [u8; 32],
    ) -> Result<Self, MboError> {
        let mut result = Self::decode_checkpoint(bytes, limit, market, durable_source)?;
        for event in suffix {
            if event.sequence > durable_source {
                return Err(MboError::InvalidCheckpoint);
            }
            result.apply(event)?;
        }
        if result.mbo.watermarks().0 != durable_source || result.digest() != expected_digest {
            return Err(MboError::InvalidCheckpoint);
        }
        Ok(result)
    }
    /// 捕获管理信号字段；调用方负责迁移屏障和实际发布。
    pub fn migration_boundary(
        &self,
        old_shard_id: ShardId,
        new_shard_id: ShardId,
        route_epoch: u64,
    ) -> MarketMigrationBoundary {
        let (source, mbo) = self.mbo.watermarks();
        MarketMigrationBoundary {
            market_id: self.market,
            old_shard_id,
            new_shard_id,
            route_epoch,
            last_market_event_seq: source,
            last_feed_seq: FeedSeqSnapshot {
                mbo,
                mbp: self.mbp_seq,
                bbo: self.bbo_seq,
                trade: self.trade_seq,
            },
        }
    }
    /// 在单次只读调用中捕获逐订单状态和水位；路由纪元须由调用方在同一屏障确认。
    pub fn l3_snapshot(&self, route_epoch: u64) -> L3Snapshot {
        L3Snapshot {
            builder: self.mbo.clone(),
            route_epoch,
        }
    }
    /// 在单次只读调用中捕获价档与对应水位；路由纪元须由调用方在同一屏障确认。
    pub fn l2_snapshot(&self, route_epoch: u64) -> L2Snapshot {
        L2Snapshot {
            market_id: self.market,
            route_epoch,
            feed_seq: self.mbp_seq,
            source_seq: self.mbo.watermarks().0,
            bids: self.bids.clone(),
            asks: self.asks.clone(),
        }
    }
    /// 编码有界检查点；调用方负责原子持久化并保留可信源前缀。
    pub fn encode_checkpoint(&self, limit: usize) -> Result<Vec<u8>, MboError> {
        use matching_domain::snapshot_wire::SnapshotEncoder;
        use sha2::{Digest, Sha256};
        let mbo = self.mbo.encode_checkpoint(limit)?;
        let mut w = SnapshotEncoder::new(limit);
        w.raw_bytes(b"FDB1")
            .and_then(|_| w.bytes(&mbo))
            .and_then(|_| w.u64(self.mbp_seq))
            .and_then(|_| w.u64(self.bbo_seq))
            .and_then(|_| w.u64(self.trade_seq))
            .map_err(|_| MboError::InvalidCheckpoint)?;
        let body = w.finish();
        let hash = Sha256::digest(&body);
        let mut w = SnapshotEncoder::new(limit);
        w.raw_bytes(&body)
            .and_then(|_| w.raw_bytes(&hash))
            .map_err(|_| MboError::InvalidCheckpoint)?;
        Ok(w.finish())
    }

    /// 用调用方提供的可信持久源水位验证并恢复检查点。
    pub fn decode_checkpoint(
        bytes: &[u8],
        limit: usize,
        market: MarketId,
        durable_source: u64,
    ) -> Result<Self, MboError> {
        use matching_domain::snapshot_wire::SnapshotDecoder;
        use sha2::{Digest, Sha256};
        let n = bytes
            .len()
            .checked_sub(32)
            .ok_or(MboError::InvalidCheckpoint)?;
        if bytes.len() > limit || Sha256::digest(&bytes[..n]).as_slice() != &bytes[n..] {
            return Err(MboError::InvalidCheckpoint);
        }
        let mut r =
            SnapshotDecoder::new(&bytes[..n], limit).map_err(|_| MboError::InvalidCheckpoint)?;
        if r.u32().map_err(|_| MboError::InvalidCheckpoint)? != u32::from_be_bytes(*b"FDB1") {
            return Err(MboError::InvalidCheckpoint);
        }
        let mbo = MboBuilder::decode_checkpoint(
            r.bytes().map_err(|_| MboError::InvalidCheckpoint)?,
            limit,
            market,
            durable_source,
        )?;
        let mbp_seq = r.u64().map_err(|_| MboError::InvalidCheckpoint)?;
        let bbo_seq = r.u64().map_err(|_| MboError::InvalidCheckpoint)?;
        let trade_seq = r.u64().map_err(|_| MboError::InvalidCheckpoint)?;
        r.finish().map_err(|_| MboError::InvalidCheckpoint)?;
        let (source, mbo_seq) = mbo.watermarks();
        if mbp_seq != mbo_seq || bbo_seq > mbp_seq || trade_seq > source.saturating_sub(mbo_seq) {
            return Err(MboError::InvalidCheckpoint);
        }
        let bids = mbo.visible_depth(Side::Buy)?;
        let asks = mbo.visible_depth(Side::Sell)?;
        Ok(Self {
            market,
            mbo,
            bids,
            asks,
            mbp_seq,
            bbo_seq,
            trade_seq,
        })
    }

    /// 计算包含全部水位、可见订单和双侧价档的确定性摘要。
    pub fn digest(&self) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(b"tide-feed-state-v1\0");
        h.update(self.mbo.digest());
        h.update(self.mbp_seq.to_be_bytes());
        h.update(self.bbo_seq.to_be_bytes());
        h.update(self.trade_seq.to_be_bytes());
        for (side, levels) in [(0_u8, &self.bids), (1_u8, &self.asks)] {
            h.update([side]);
            for (price, qty) in levels {
                h.update([1]);
                h.update(price.get().to_be_bytes());
                h.update(qty.to_be_bytes());
            }
            h.update([0]);
        }
        h.finalize().into()
    }
    /// 从空市场和零序号开始构建。
    pub fn new(market: MarketId) -> Self {
        Self {
            market,
            mbo: MboBuilder::new(market),
            bids: Vec::new(),
            asks: Vec::new(),
            mbp_seq: 0,
            bbo_seq: 0,
            trade_seq: 0,
        }
    }

    /// 按价格优先顺序返回公开可见深度。
    pub fn depth(&self, side: Side) -> Vec<(Price, u64)> {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
        .clone()
    }

    /// 顺序处理源事件；错误时所有状态和序号均保持不变。
    /// 不含逻辑时间的兼容成交载荷无法构成公开成交，将返回错误。
    pub fn apply(&mut self, event: &SourceEvent) -> Result<FeedOutput, MboError> {
        if matches!(event.payload, SourcePayload::TradeOccurred(_)) {
            return Err(MboError::InvalidSource);
        }
        let mut next = self.clone();
        let mbo = next.mbo.apply(event)?;
        let bids = next.mbo.visible_depth(Side::Buy)?;
        let asks = next.mbo.visible_depth(Side::Sell)?;
        let mut output = FeedOutput {
            mbo,
            ..FeedOutput::default()
        };
        for (side, old, new) in [
            (Side::Buy, &self.bids, &bids),
            (Side::Sell, &self.asks, &asks),
        ] {
            let mut levels = BTreeMap::new();
            for &(price, qty) in old {
                levels.insert(price, (qty, 0));
            }
            for &(price, qty) in new {
                levels.entry(price).or_insert((0, 0)).1 = qty;
            }
            for (price, (before, after)) in levels {
                if before != after {
                    if output.mbp.is_some() {
                        return Err(MboError::InvalidSource);
                    }
                    next.mbp_seq = next.mbp_seq.checked_add(1).ok_or(MboError::Overflow)?;
                    output.mbp = Some(MbpMessage {
                        market_id: self.market,
                        feed_seq: next.mbp_seq,
                        source_seq: event.sequence,
                        side,
                        price,
                        visible: after,
                    });
                }
            }
        }
        let best = |v: &Vec<(Price, u64)>| {
            v.first().map(|(price, visible)| BestLevel {
                price: *price,
                visible: *visible,
            })
        };
        if best(&self.bids) != best(&bids) || best(&self.asks) != best(&asks) {
            next.bbo_seq = next.bbo_seq.checked_add(1).ok_or(MboError::Overflow)?;
            output.bbo = Some(BboMessage {
                market_id: self.market,
                feed_seq: next.bbo_seq,
                source_seq: event.sequence,
                bid: best(&bids),
                ask: best(&asks),
            });
        }
        if let SourcePayload::TimedTradeOccurred { trade, engine_time } = &event.payload {
            next.trade_seq = next.trade_seq.checked_add(1).ok_or(MboError::Overflow)?;
            output.trade = Some(PublicTrade {
                trade_id: trade.trade_id,
                market_id: self.market,
                feed_seq: next.trade_seq,
                source_seq: event.sequence,
                price: trade.price,
                qty: trade.qty,
                maker_side: trade.maker_side,
                engine_time: *engine_time,
            });
        }
        next.bids = bids;
        next.asks = asks;
        *self = next;
        Ok(output)
    }
}
