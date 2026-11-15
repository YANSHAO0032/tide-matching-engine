//! 从有序市场源构造独立的公开价档、最优报价和成交序列。
use crate::{MboBuilder, MboError, MboMessage, SourceEvent, SourcePayload};
use matching_domain::{MarketId, Price, Qty, Side, TradeId};
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
