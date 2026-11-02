//! 进程内分片与市场的唯一持有边界。
use matching_domain::{ClusterId, MarketId, ShardId};
use std::collections::BTreeSet;
use std::sync::{Mutex, OnceLock};

/// 进程内当前已认领的分片和市场身份集合。
#[derive(Default)]
struct Active {
    /// 活跃的集群和分片身份，避免独立序列分配器产生重复成交身份。
    shards: BTreeSet<(ClusterId, ShardId)>,
    /// 活跃的集群和市场身份，避免重复订单簿所有者。
    markets: BTreeSet<(ClusterId, MarketId)>,
}

fn active() -> &'static Mutex<Active> {
    static ACTIVE: OnceLock<Mutex<Active>> = OnceLock::new();
    ACTIVE.get_or_init(|| Mutex::new(Active::default()))
}

/// 随工作线程结束释放的所有权，锁仅保护入口元数据。
pub(crate) struct MarketOwnership {
    /// 已认领的集群与分片身份。
    shard: (ClusterId, ShardId),
    /// 该线程已认领的集群与市场身份。
    markets: Vec<(ClusterId, MarketId)>,
}

impl MarketOwnership {
    /// 原子认领分片与市场，重复身份不得启动第二个所有者。
    pub(crate) fn acquire(
        cluster: ClusterId,
        shard: ShardId,
        markets: impl Iterator<Item = MarketId>,
    ) -> Option<Self> {
        let shard = (cluster, shard);
        let markets: Vec<_> = markets.map(|market| (cluster, market)).collect();
        let mut active = active().lock().expect("市场所有权锁损坏");
        if active.shards.contains(&shard) || markets.iter().any(|key| active.markets.contains(key))
        {
            return None;
        }
        assert!(active.shards.insert(shard));
        for key in &markets {
            assert!(active.markets.insert(*key));
        }
        Some(Self { shard, markets })
    }
}

impl Drop for MarketOwnership {
    fn drop(&mut self) {
        let mut active = active().lock().expect("市场所有权锁损坏");
        assert!(active.shards.remove(&self.shard));
        for key in &self.markets {
            assert!(active.markets.remove(key));
        }
    }
}
