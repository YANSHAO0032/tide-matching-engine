use crate::{Price, RejectReason};
use std::collections::BTreeMap;

/// 可驱动止损触发的确定性价格参考来源。
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PriceReferenceKind {
    /// 最近一笔成交价格。
    LastTrade = 1,
    /// 市场标记价格。
    MarkPrice = 2,
    /// 外部指数价格。
    IndexPrice = 3,
    /// 开市时使用的静态参考价格，不能驱动止损触发。
    StaticOpening = 4,
}

/// 一次价格参考更新的稳定输入。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PriceReference {
    /// 参考来源类别。
    pub kind: PriceReferenceKind,
    /// 同一来源的单调序号，用于拒绝乱序更新。
    pub source_seq: u64,
    /// 已验证为严格正的参考价格。
    pub price: Price,
}

/// 各参考来源最近一次已接受的价格和序号。
///
/// 该状态只保存确定性输入，不读取本机时间，也不决定订单簿的触发顺序。
/// 撮合实现据此判断本次更新是否真的跨越了某个止损价位。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PriceReferenceState {
    /// 每种参考来源最后接受的更新。
    latest: BTreeMap<PriceReferenceKind, PriceReference>,
}

impl PriceReferenceState {
    /// 接受一个严格递增的同来源更新，并返回更新前的价格。
    ///
    /// 首次更新没有前值，调用方应只建立基线而不据此推断价格跨越。
    pub fn advance(&mut self, reference: PriceReference) -> Result<Option<Price>, RejectReason> {
        if let Some(previous) = self.latest.get(&reference.kind)
            && reference.source_seq <= previous.source_seq
        {
            return Err(RejectReason::StaleReferencePriceUpdate);
        }
        let previous = self
            .latest
            .insert(reference.kind, reference)
            .map(|value| value.price);
        Ok(previous)
    }

    /// 返回一个来源已接受的最新更新。
    pub fn latest(&self, kind: PriceReferenceKind) -> Option<PriceReference> {
        self.latest.get(&kind).copied()
    }

    /// 为内部真实成交产生下一条最近成交价参考。
    pub fn next_last_trade(&self, price: Price) -> Result<PriceReference, RejectReason> {
        let source_seq = match self.latest(PriceReferenceKind::LastTrade) {
            Some(previous) => previous
                .source_seq
                .checked_add(1)
                .ok_or(RejectReason::ArithmeticOverflow)?,
            None => 1,
        };
        Ok(PriceReference {
            kind: PriceReferenceKind::LastTrade,
            source_seq,
            price,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn price_reference_kind_codes_and_source_sequence_are_stable() {
        assert_eq!(PriceReferenceKind::LastTrade as u8, 1);
        assert_eq!(PriceReferenceKind::MarkPrice as u8, 2);
        assert_eq!(PriceReferenceKind::IndexPrice as u8, 3);
        assert_eq!(PriceReferenceKind::StaticOpening as u8, 4);

        let mut state = PriceReferenceState::default();
        let first = PriceReference {
            kind: PriceReferenceKind::MarkPrice,
            source_seq: 7,
            price: Price::try_new(100).unwrap(),
        };
        assert_eq!(state.advance(first), Ok(None));
        assert_eq!(
            state.advance(first),
            Err(RejectReason::StaleReferencePriceUpdate)
        );
        assert_eq!(
            state.advance(PriceReference {
                source_seq: 8,
                price: Price::try_new(101).unwrap(),
                ..first
            }),
            Ok(Some(Price::try_new(100).unwrap()))
        );
    }
}
