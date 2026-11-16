//! 公开行情构建器的水位与缺口观测，不参与撮合。
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

/// 被客户端游标消费的公开行情类型。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeedKind {
    /// 逐订单流。
    Mbo,
    /// 价档流。
    Mbp,
    /// 最优报价流。
    Bbo,
    /// 公开成交流。
    Trade,
}
impl FeedKind {
    /// 返回固定的有界标签位置。
    pub(crate) fn index(self) -> usize {
        match self {
            Self::Mbo => 0,
            Self::Mbp => 1,
            Self::Bbo => 2,
            Self::Trade => 3,
        }
    }
}

/// 客户端游标报告的缺口计数，不代表服务端持久状态。
pub struct FeedGapTelemetry {
    /// 所属行情类别。
    kind: FeedKind,
    /// 实际遇到的本流缺口次数。
    gaps: AtomicU64,
    /// 计数耗尽标志。
    overflow: AtomicBool,
}
impl FeedGapTelemetry {
    /// 读取当前类别与计数。
    pub(crate) fn snapshot(&self) -> (FeedKind, u64, bool) {
        (
            self.kind,
            self.gaps.load(Ordering::Relaxed),
            self.overflow.load(Ordering::Relaxed),
        )
    }
}

/// 包装真实客户端游标，记录本流序号缺口。
pub struct FeedCursorMonitor {
    /// 负责去重和缺口判定的实际游标。
    cursor: crate::FeedCursor,
    /// 可供健康指标登记的观测句柄。
    telemetry: Arc<FeedGapTelemetry>,
}
impl FeedCursorMonitor {
    /// 根据可信本流起点创建游标和观测状态。
    pub fn new(market: matching_domain::MarketId, sequence: u64, kind: FeedKind) -> Self {
        Self {
            cursor: crate::FeedCursor::new(market, sequence),
            telemetry: Arc::new(FeedGapTelemetry {
                kind,
                gaps: AtomicU64::new(0),
                overflow: AtomicBool::new(false),
            }),
        }
    }
    /// 返回可显式登记的只读观测句柄。
    pub fn telemetry(&self) -> Arc<FeedGapTelemetry> {
        Arc::clone(&self.telemetry)
    }
    /// 应用本流序号；只统计实际缺口，不把重复或市场错误当成缺口。
    pub fn accept(
        &mut self,
        market: matching_domain::MarketId,
        sequence: u64,
    ) -> Result<bool, crate::ResumeError> {
        let result = self.cursor.accept(market, sequence);
        if matches!(result, Err(crate::ResumeError::Gap))
            && self
                .telemetry
                .gaps
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                    old.checked_add(1)
                })
                .is_err()
        {
            self.telemetry.overflow.store(true, Ordering::Relaxed);
        }
        result
    }
}

/// 一个市场的行情构建器观测状态；不提供持久性证明。
pub struct FeedTelemetry {
    /// 已配置的市场身份。
    market: matching_domain::MarketId,
    /// 已接受的最后市场源序号。
    source: AtomicU64,
    /// 四条公开行情流各自的最后序号。
    feeds: [AtomicU64; 4],
    /// 观察到的源序号缺口次数。
    gaps: AtomicU64,
    /// 计数溢出标志。
    overflow: AtomicBool,
    /// 构建器仍可供消费。
    valid: AtomicBool,
}

/// 单写者持有的行情构建器与其观测状态。
pub struct FeedMonitor {
    /// 顺序处理已提交源的实际构建器。
    builder: crate::FeedBuilder,
    /// 供观测端读取的原子水位。
    telemetry: Arc<FeedTelemetry>,
}

impl FeedMonitor {
    /// 接管由调用方验证的构建器；恢复状态可信性仍由调用方保证。
    pub fn new(builder: crate::FeedBuilder) -> Self {
        let (source, seq) = builder.watermarks();
        let telemetry = Arc::new(FeedTelemetry {
            market: builder.market_id(),
            source: AtomicU64::new(source),
            feeds: [seq.mbo, seq.mbp, seq.bbo, seq.trade].map(AtomicU64::new),
            gaps: AtomicU64::new(0),
            overflow: AtomicBool::new(false),
            valid: AtomicBool::new(true),
        });
        Self { builder, telemetry }
    }

    /// 返回只读观测句柄；构建器销毁或出现错误后标记为不可用。
    pub fn telemetry(&self) -> Arc<FeedTelemetry> {
        Arc::clone(&self.telemetry)
    }

    /// 顺序应用源事件；失败后停止消费，须从可信前缀重建构建器。
    pub fn apply(
        &mut self,
        event: &crate::SourceEvent,
    ) -> Result<crate::FeedOutput, crate::MboError> {
        if !self.telemetry.valid.load(Ordering::Acquire) {
            return Err(crate::MboError::InvalidSource);
        }
        match self.builder.apply(event) {
            Ok(output) => {
                let (source, seq) = self.builder.watermarks();
                for (target, value) in self
                    .telemetry
                    .feeds
                    .iter()
                    .zip([seq.mbo, seq.mbp, seq.bbo, seq.trade])
                {
                    target.store(value, Ordering::Release);
                }
                self.telemetry.source.store(source, Ordering::Release);
                Ok(output)
            }
            Err(error) => {
                if error == crate::MboError::SourceGap
                    && self
                        .telemetry
                        .gaps
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
                        .is_err()
                {
                    self.telemetry.overflow.store(true, Ordering::Relaxed);
                }
                self.telemetry.valid.store(false, Ordering::Release);
                Err(error)
            }
        }
    }
}

impl Drop for FeedMonitor {
    fn drop(&mut self) {
        self.telemetry.valid.store(false, Ordering::Release);
    }
}

impl FeedTelemetry {
    /// 市场身份用于受配置约束的标签。
    pub(crate) fn market_id(&self) -> matching_domain::MarketId {
        self.market
    }
    /// 返回瞬时观测值，多个字段不构成原子快照。
    pub(crate) fn snapshot(&self) -> (u64, [u64; 4], u64, bool, bool) {
        (
            self.source.load(Ordering::Acquire),
            std::array::from_fn(|i| self.feeds[i].load(Ordering::Acquire)),
            self.gaps.load(Ordering::Relaxed),
            self.valid.load(Ordering::Acquire),
            self.overflow.load(Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_gap_counter_reports_exhaustion_without_wrapping() {
        let mut cursor = FeedCursorMonitor::new(matching_domain::MarketId(1), 0, FeedKind::Bbo);
        cursor.telemetry.gaps.store(u64::MAX, Ordering::Relaxed);
        assert_eq!(
            cursor.accept(matching_domain::MarketId(1), 2),
            Err(crate::ResumeError::Gap)
        );
        assert_eq!(cursor.telemetry.snapshot(), (FeedKind::Bbo, u64::MAX, true));
    }
}
