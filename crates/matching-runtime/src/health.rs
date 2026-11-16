//! 健康汇总：依赖报告默认失败，分片状态来自实际工作线程。
use std::sync::atomic::{AtomicU8, Ordering};

/// 由持有对应资源的外围组件报告的必要条件。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HealthDependency {
    /// 本地恢复已完成。
    Recovery,
    /// 所需持久存储健康。
    Persistence,
    /// 当前角色所需控制权有效。
    Control,
}

impl HealthDependency {
    /// 返回该条件的独立位。
    fn bit(self) -> u8 {
        match self {
            Self::Recovery => 1,
            Self::Persistence => 2,
            Self::Control => 4,
        }
    }
}

/// 面向单节点的健康状态；依赖报告由实际资源所有者负责维护。
pub struct Health {
    /// 需要同时运行的分片，不允许空集合。
    shards: Vec<crate::ShardHandle>,
    /// 尚未报告成功的依赖默认不健康。
    healthy: AtomicU8,
    /// 让依赖失效与网关入队在线性化边界内互斥。
    admission: std::sync::Mutex<()>,
    /// 当前角色是否要求每个分片持有经过校验的主节点租约。
    require_primary: bool,
    /// 显式配置且数量受限的行情构建器观测。
    feeds: Vec<std::sync::Arc<crate::FeedTelemetry>>,
    /// 显式登记的有限数量客户端游标指标。
    feed_cursors: Vec<std::sync::Arc<crate::FeedGapTelemetry>>,
    /// 显式登记且数量受限的市场。
    priority_markets: Vec<matching_domain::MarketId>,
}

impl Health {
    /// 建立健康检查；没有分片不能被认作健康节点。
    pub fn new(shards: Vec<crate::ShardHandle>) -> Result<Self, &'static str> {
        if shards.is_empty()
            || shards
                .iter()
                .enumerate()
                .any(|(i, shard)| shards[..i].iter().any(|other| shard.same_worker(other)))
        {
            return Err("必须提供不重复的分片");
        }
        Ok(Self {
            shards,
            healthy: AtomicU8::new(0),
            admission: std::sync::Mutex::new(()),
            require_primary: false,
            feeds: Vec::new(),
            feed_cursors: Vec::new(),
            priority_markets: Vec::new(),
        })
    }

    /// 显式登记游标缺口指标，拒绝重复句柄并限制总数量。
    pub fn with_feed_cursors(
        mut self,
        cursors: Vec<std::sync::Arc<crate::FeedGapTelemetry>>,
        max_cursors: usize,
    ) -> Result<Self, &'static str> {
        if max_cursors == 0
            || max_cursors > 1024
            || cursors.len() > max_cursors
            || cursors.iter().enumerate().any(|(i, cursor)| {
                cursors[..i]
                    .iter()
                    .any(|other| std::sync::Arc::ptr_eq(cursor, other))
            })
        {
            return Err("行情游标重复或超出配置上限");
        }
        self.feed_cursors = cursors;
        Ok(self)
    }

    /// 显式登记优先级指标，限制标签基数并拒绝重复市场。
    pub fn with_priority_markets(
        mut self,
        markets: Vec<matching_domain::MarketId>,
        max_markets: usize,
    ) -> Result<Self, &'static str> {
        if max_markets == 0
            || max_markets > 1024
            || markets.len() > max_markets
            || markets
                .iter()
                .enumerate()
                .any(|(index, market)| markets[..index].contains(market))
        {
            return Err("优先级指标市场重复或超出配置上限");
        }
        self.priority_markets = markets;
        Ok(self)
    }

    /// 显式登记行情指标；数量上限限制 market_id 标签基数。
    pub fn with_feeds(
        mut self,
        feeds: Vec<std::sync::Arc<crate::FeedTelemetry>>,
        max_markets: usize,
    ) -> Result<Self, &'static str> {
        if max_markets == 0
            || max_markets > 1024
            || feeds.len() > max_markets
            || feeds.iter().enumerate().any(|(index, feed)| {
                feeds[..index]
                    .iter()
                    .any(|other| other.market_id() == feed.market_id())
            })
        {
            return Err("行情指标市场重复或超出配置上限");
        }
        self.feeds = feeds;
        Ok(self)
    }

    /// 创建需要本地持久写入与有效主节点租约的健康检查。
    ///
    /// 租约由分片持有并在启动时校验身份；此接口不提供跨主机租约。
    pub fn new_primary(shards: Vec<crate::ShardHandle>) -> Result<Self, &'static str> {
        let mut health = Self::new(shards)?;
        if health.shards.iter().any(|shard| {
            let state = shard.observation();
            state.wal_append.is_none()
                || !state.primary_lease_held
                || !state.thread_running
                || !state.ingress_open
                || state.failed
        }) {
            return Err("主节点要求运行中的持久分片及有效本机租约");
        }
        health.require_primary = true;
        Ok(health)
    }

    /// 验证网关的分片集合与健康检查来源完全一致。
    pub(crate) fn covers(&self, shards: &[crate::ShardHandle]) -> bool {
        shards.len() == self.shards.len()
            && self.shards.iter().all(|expected| {
                shards
                    .iter()
                    .filter(|shard| expected.same_worker(shard))
                    .count()
                    == 1
            })
    }

    /// 资源所有者在成功恢复、失效或关闭时更新对应状态。
    pub fn set_healthy(&self, dependency: HealthDependency, healthy: bool) {
        let Ok(_guard) = self.admission.lock() else {
            self.healthy.store(0, Ordering::Release);
            return;
        };
        if healthy {
            self.healthy.fetch_or(dependency.bit(), Ordering::AcqRel);
        } else {
            self.healthy.fetch_and(!dependency.bit(), Ordering::AcqRel);
        }
    }

    /// 在依赖更新的同一边界内执行一次非阻塞入队，不允许包含网络或磁盘操作。
    pub(crate) fn admit<T>(&self, enqueue: impl FnOnce() -> T) -> Option<T> {
        let _guard = self.admission.lock().ok()?;
        self.ready().then(enqueue)
    }

    /// 所有关键工作线程仍在运行且未报告不可恢复错误。
    pub fn live(&self) -> bool {
        self.shards.iter().all(|shard| {
            let state = shard.observation();
            state.thread_running && !state.failed
        })
    }

    /// 任一受监测分片仅驻留内存时报告临时性；不代表跨主机高可用。
    pub fn durability(&self) -> &'static str {
        if self
            .shards
            .iter()
            .any(|shard| shard.observation().wal_append.is_none())
        {
            "ephemeral"
        } else {
            "local"
        }
    }

    /// 依赖健康且所有分片入口仍接受请求时才允许接收新流量。
    pub fn ready(&self) -> bool {
        !self.admission.is_poisoned()
            && self.healthy.load(Ordering::Acquire) == 7
            && self.shards.iter().all(|shard| {
                let state = shard.observation();
                state.thread_running
                    && !state.failed
                    && state.ingress_open
                    && !state.shard_halted
                    && (!self.require_primary || state.primary_lease_held)
            })
    }

    /// 返回当前分片队列合计与健康状态的 Prometheus 文本，不使用高基数标签。
    ///
    /// 队列长度为并发观测值；汇总溢出时返回错误而不输出伪造数值。
    pub fn prometheus(&self) -> Result<String, &'static str> {
        let mut depth = 0_usize;
        let mut capacity = 0_usize;
        let mut commands = 0_u64;
        let mut rejects = 0_u64;
        let mut priority_exhaustions = 0_u64;
        let mut trades = 0_u64;
        let mut diagnostics_dropped = 0_u64;
        let mut overflow = false;
        let mut latency = [0_u64; 7];
        let mut latency_sum = 0_u64;
        let mut command_latency = [0_u64; 7];
        let mut command_latency_sum = 0_u64;
        let mut snapshots = [0_u64; 7];
        let mut snapshot_sum = 0_u64;
        let mut snapshot_failures = 0_u64;
        let mut replay_records = 0_u64;
        let mut replay_buckets = [0_u64; 7];
        let mut replay_sum_ms = 0_u64;
        let mut wal_append = matching_persistence::LatencySnapshot::default();
        let mut queue_wait = matching_persistence::LatencySnapshot::default();
        let mut wal_sync = matching_persistence::LatencySnapshot::default();
        let mut batch_size = matching_persistence::BatchSizeSnapshot::default();
        let mut batch_wait = matching_persistence::LatencySnapshot::default();
        let mut durable_present = false;
        let feed_snapshots: Vec<_> = self
            .feeds
            .iter()
            .map(|feed| (feed.market_id(), feed.snapshot()))
            .collect();
        overflow |= feed_snapshots
            .iter()
            .any(|(_, (_, _, _, _, exhausted))| *exhausted);
        let mut feed_gaps = [0_u64; 4];
        let mut feed_present = [false; 4];
        for cursor in &self.feed_cursors {
            let (kind, gaps, exhausted) = cursor.snapshot();
            let index = kind.index();
            feed_gaps[index] = feed_gaps[index]
                .checked_add(gaps)
                .ok_or("行情缺口计数汇总溢出")?;
            feed_present[index] = true;
            overflow |= exhausted;
        }
        for shard in &self.shards {
            let state = shard.observation();
            depth = depth.checked_add(state.queue_depth).ok_or("队列深度溢出")?;
            capacity = capacity
                .checked_add(state.queue_capacity)
                .ok_or("队列容量溢出")?;
            commands = commands
                .checked_add(state.commands)
                .ok_or("命令指标汇总溢出")?;
            rejects = rejects
                .checked_add(state.rejects)
                .ok_or("拒绝指标汇总溢出")?;
            priority_exhaustions = priority_exhaustions
                .checked_add(state.priority_exhaustions)
                .ok_or("优先级耗尽计数溢出")?;
            trades = trades.checked_add(state.trades).ok_or("成交指标汇总溢出")?;
            diagnostics_dropped = diagnostics_dropped
                .checked_add(state.diagnostics_dropped)
                .ok_or("诊断丢弃计数溢出")?;
            overflow |= state.metrics_overflow;
            merge_latency(&mut queue_wait, state.queue_wait)?;
            overflow |= state.queue_wait.overflow;
            for (total, value) in latency.iter_mut().zip(state.match_latency_buckets) {
                *total = total.checked_add(value).ok_or("延迟桶汇总溢出")?;
            }
            latency_sum = latency_sum
                .checked_add(state.match_latency_sum_us)
                .ok_or("延迟总和溢出")?;
            for (total, value) in command_latency
                .iter_mut()
                .zip(state.command_latency_buckets)
            {
                *total = total.checked_add(value).ok_or("命令延迟桶汇总溢出")?;
            }
            command_latency_sum = command_latency_sum
                .checked_add(state.command_latency_sum_us)
                .ok_or("命令延迟汇总溢出")?;
            for (total, value) in snapshots.iter_mut().zip(state.snapshot_latency_buckets) {
                *total = total.checked_add(value).ok_or("快照桶汇总溢出")?;
            }
            snapshot_sum = snapshot_sum
                .checked_add(state.snapshot_latency_sum_ms)
                .ok_or("快照时延汇总溢出")?;
            snapshot_failures = snapshot_failures
                .checked_add(state.snapshot_failures)
                .ok_or("快照失败计数溢出")?;
            replay_records = replay_records
                .checked_add(state.replay_records)
                .ok_or("重放记录计数溢出")?;
            for (total, value) in replay_buckets.iter_mut().zip(state.replay_buckets) {
                *total = total.checked_add(value).ok_or("重放时延桶汇总溢出")?;
            }
            replay_sum_ms = replay_sum_ms
                .checked_add(state.replay_sum_ms)
                .ok_or("重放时延汇总溢出")?;
            if let (Some(append), Some(sync)) = (state.wal_append, state.wal_sync) {
                durable_present = true;
                merge_latency(&mut wal_append, append)?;
                merge_latency(&mut wal_sync, sync)?;
                overflow |= append.overflow || sync.overflow;
            }
            if let (Some(size), Some(wait)) = (state.wal_batch_size, state.wal_batch_wait) {
                for (total, value) in batch_size.buckets.iter_mut().zip(size.buckets) {
                    *total = total.checked_add(value).ok_or("批次桶汇总溢出")?;
                }
                batch_size.sum = batch_size
                    .sum
                    .checked_add(size.sum)
                    .ok_or("批次数汇总溢出")?;
                merge_latency(&mut batch_wait, wait)?;
                overflow |= size.overflow || wait.overflow;
            }
        }
        let mut output = format!(
            "# TYPE engine_queue_depth gauge\nengine_queue_depth {depth}\n# TYPE engine_queue_capacity gauge\nengine_queue_capacity {capacity}\n# TYPE engine_commands_total counter\nengine_commands_total {commands}\n# TYPE engine_reject_total counter\nengine_reject_total {rejects}\n# TYPE engine_trades_total counter\nengine_trades_total {trades}\n# TYPE engine_metrics_overflow gauge\nengine_metrics_overflow {}\n# TYPE engine_live gauge\nengine_live {}\n# TYPE engine_ready gauge\nengine_ready {}\n",
            u8::from(overflow),
            u8::from(self.live()),
            u8::from(self.ready())
        );
        use std::fmt::Write as _;
        writeln!(&mut output, "# TYPE priority_space_exhaustion_total counter\npriority_space_exhaustion_total {priority_exhaustions}").map_err(|_| "指标格式化失败")?;
        writeln!(&mut output, "# TYPE engine_diagnostics_dropped_total counter\nengine_diagnostics_dropped_total {diagnostics_dropped}").map_err(|_| "指标格式化失败")?;
        if feed_present.iter().any(|value| *value) {
            output.push_str("# TYPE market_data_feed_gap_total counter\n");
            for (index, name) in ["mbo", "mbp", "bbo", "trade"].into_iter().enumerate() {
                if feed_present[index] {
                    writeln!(
                        &mut output,
                        "market_data_feed_gap_total{{feed=\"{name}\"}} {}",
                        feed_gaps[index]
                    )
                    .map_err(|_| "指标格式化失败")?;
                }
            }
        }
        if !feed_snapshots.is_empty() {
            output.push_str("# TYPE market_event_last_seq gauge\n");
            for (market, (source, _, _, _, _)) in &feed_snapshots {
                writeln!(
                    &mut output,
                    "market_event_last_seq{{market_id=\"{}\"}} {source}",
                    market.0
                )
                .map_err(|_| "指标格式化失败")?;
            }
            output.push_str("# TYPE market_event_sequence_gap_total counter\n");
            for (market, (_, _, gaps, _, _)) in &feed_snapshots {
                writeln!(
                    &mut output,
                    "market_event_sequence_gap_total{{market_id=\"{}\"}} {gaps}",
                    market.0
                )
                .map_err(|_| "指标格式化失败")?;
            }
            output.push_str("# TYPE market_data_feed_seq_last gauge\n");
            for (market, (_, feeds, _, _, _)) in &feed_snapshots {
                for (name, seq) in ["mbo", "mbp", "bbo", "trade"].into_iter().zip(feeds) {
                    writeln!(
                        &mut output,
                        "market_data_feed_seq_last{{market_id=\"{}\",feed=\"{name}\"}} {seq}",
                        market.0
                    )
                    .map_err(|_| "指标格式化失败")?;
                }
            }
            output.push_str("# TYPE market_data_builder_valid gauge\n");
            for (market, (_, _, _, valid, _)) in &feed_snapshots {
                writeln!(
                    &mut output,
                    "market_data_builder_valid{{market_id=\"{}\"}} {}",
                    market.0,
                    u8::from(*valid)
                )
                .map_err(|_| "指标格式化失败")?;
            }
        }
        let micros = ["10", "100", "1000", "10000", "100000", "1000000", "+Inf"];
        append_histogram(
            &mut output,
            "engine_command_latency_us",
            command_latency,
            command_latency_sum,
            micros,
        )?;
        append_histogram(
            &mut output,
            "engine_match_latency_us",
            latency,
            latency_sum,
            micros,
        )?;
        append_histogram(
            &mut output,
            "engine_queue_wait_us",
            queue_wait.buckets,
            queue_wait.sum_us,
            micros,
        )?;
        append_histogram(
            &mut output,
            "engine_snapshot_duration_ms",
            snapshots,
            snapshot_sum,
            ["1", "10", "100", "1000", "10000", "60000", "+Inf"],
        )?;
        writeln!(&mut output, "# TYPE engine_snapshot_failures_total counter\nengine_snapshot_failures_total {snapshot_failures}").map_err(|_| "指标格式化失败")?;
        writeln!(&mut output, "# TYPE engine_replay_records_total counter\nengine_replay_records_total {replay_records}").map_err(|_| "指标格式化失败")?;
        append_histogram(
            &mut output,
            "engine_replay_duration_ms",
            replay_buckets,
            replay_sum_ms,
            ["1", "10", "100", "1000", "10000", "60000", "+Inf"],
        )?;
        if durable_present {
            append_histogram(
                &mut output,
                "wal_group_commit_batch_size",
                batch_size.buckets,
                batch_size.sum,
                ["1", "2", "4", "8", "16", "64", "+Inf"],
            )?;
            append_histogram(
                &mut output,
                "wal_group_commit_wait_us",
                batch_wait.buckets,
                batch_wait.sum_us,
                micros,
            )?;
            append_histogram(
                &mut output,
                "engine_wal_append_latency_us",
                wal_append.buckets,
                wal_append.sum_us,
                micros,
            )?;
            append_histogram(
                &mut output,
                "engine_wal_fsync_latency_us",
                wal_sync.buckets,
                wal_sync.sum_us,
                micros,
            )?;
        }
        let mut books = Some((0_usize, 0_usize));
        for shard in &self.shards {
            let observed = shard.book_counts().ok().and_then(|receiver| {
                receiver
                    .recv_timeout(std::time::Duration::from_secs(1))
                    .ok()
                    .flatten()
            });
            books = books
                .zip(observed)
                .and_then(|((orders, levels), (new_orders, new_levels))| {
                    Some((
                        orders.checked_add(new_orders)?,
                        levels.checked_add(new_levels)?,
                    ))
                });
        }
        writeln!(
            &mut output,
            "# TYPE engine_book_metrics_available gauge\nengine_book_metrics_available {}",
            u8::from(books.is_some())
        )
        .map_err(|_| "指标格式化失败")?;
        if let Some((orders, levels)) = books {
            writeln!(&mut output, "# TYPE engine_open_orders gauge\nengine_open_orders {orders}\n# TYPE engine_price_levels gauge\nengine_price_levels {levels}").map_err(|_| "指标格式化失败")?;
        }
        if !self.priority_markets.is_empty() {
            let mut observed = std::collections::BTreeMap::new();
            let mut valid = true;
            for shard in &self.shards {
                let result = shard
                    .priority_counts(self.priority_markets.clone())
                    .ok()
                    .and_then(|rx| rx.recv_timeout(std::time::Duration::from_secs(1)).ok());
                if let Some(result) = result {
                    for value in result {
                        if observed.insert(value.market, value).is_some() {
                            valid = false;
                        }
                    }
                } else {
                    valid = false;
                }
            }
            valid &= observed.len() == self.priority_markets.len();
            writeln!(&mut output, "# TYPE engine_priority_metrics_available gauge\nengine_priority_metrics_available {}", u8::from(valid)).map_err(|_| "指标格式化失败")?;
            if valid {
                for (name, queue) in [
                    ("market_queue_priority_next", true),
                    ("market_stop_priority_next", false),
                    ("queue_priority_allocations_total", true),
                    ("stop_priority_allocations_total", false),
                ] {
                    let kind = if name.ends_with("_total") {
                        "counter"
                    } else {
                        "gauge"
                    };
                    writeln!(&mut output, "# TYPE {name} {kind}").map_err(|_| "指标格式化失败")?;
                    for value in observed.values() {
                        let next = if queue { value.queue } else { value.stop };
                        writeln!(
                            &mut output,
                            "{name}{{market_id=\"{}\"}} {next}",
                            value.market.0
                        )
                        .map_err(|_| "指标格式化失败")?;
                    }
                }
                output.push_str("# TYPE priority_space_remaining_estimate gauge\n");
                for value in observed.values() {
                    for (kind, next) in [("queue", value.queue), ("stop", value.stop)] {
                        let remaining = value.ceiling.checked_sub(next).ok_or("优先级超出上限")?;
                        writeln!(&mut output, "priority_space_remaining_estimate{{market_id=\"{}\",kind=\"{kind}\"}} {remaining}", value.market.0).map_err(|_| "指标格式化失败")?;
                    }
                }
            }
        }
        Ok(output)
    }

    /// 处理一个有界 HTTP 观测请求；调用方负责连接超时和访问控制。
    ///
    /// 只支持 GET，响应后关闭连接；不会读取或执行请求体。
    pub fn serve_http(
        &self,
        stream: &mut (impl std::io::Read + std::io::Write),
        max_header: usize,
    ) -> std::io::Result<()> {
        use std::io::{Error, ErrorKind};
        if !(4..=65_536).contains(&max_header) {
            return Err(Error::new(ErrorKind::InvalidInput, "无效的请求头上限"));
        }
        let mut header = Vec::new();
        header
            .try_reserve_exact(max_header)
            .map_err(|_| Error::other("请求头内存不足"))?;
        while !header.ends_with(b"\r\n\r\n") {
            if header.len() == max_header {
                return Err(Error::new(ErrorKind::InvalidData, "请求头超出上限"));
            }
            let mut byte = [0];
            stream.read_exact(&mut byte)?;
            header.push(byte[0]);
        }
        let text = std::str::from_utf8(&header)
            .map_err(|_| Error::new(ErrorKind::InvalidData, "请求头编码无效"))?;
        let line = text.split("\r\n").next().unwrap_or_default();
        let mut fields = line.split(' ');
        let (method, path, version) = (fields.next(), fields.next(), fields.next());
        if fields.next().is_some() || !matches!(version, Some("HTTP/1.0" | "HTTP/1.1")) {
            return Err(Error::new(ErrorKind::InvalidData, "请求行无效"));
        }
        let (status, body, content_type) = match (method, path) {
            (Some("GET"), Some("/live")) => {
                if self.live() {
                    ("200 OK", "live\n".to_string(), "text/plain")
                } else {
                    (
                        "503 Service Unavailable",
                        "not live\n".to_string(),
                        "text/plain",
                    )
                }
            }
            (Some("GET"), Some("/ready")) => {
                if self.ready() {
                    (
                        "200 OK",
                        format!("ready\ndurability={}\n", self.durability()),
                        "text/plain",
                    )
                } else {
                    (
                        "503 Service Unavailable",
                        format!("not ready\ndurability={}\n", self.durability()),
                        "text/plain",
                    )
                }
            }
            (Some("GET"), Some("/metrics")) => (
                "200 OK",
                self.prometheus().map_err(Error::other)?,
                "text/plain; version=0.0.4",
            ),
            (Some("GET"), _) => ("404 Not Found", "not found\n".to_string(), "text/plain"),
            _ => (
                "405 Method Not Allowed",
                "method not allowed\n".to_string(),
                "text/plain",
            ),
        };
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes())
    }
}

/// 聚合互斥桶；溢出不输出不完整指标。
fn merge_latency(
    total: &mut matching_persistence::LatencySnapshot,
    value: matching_persistence::LatencySnapshot,
) -> Result<(), &'static str> {
    for (target, count) in total.buckets.iter_mut().zip(value.buckets) {
        *target = target.checked_add(count).ok_or("延迟桶汇总溢出")?;
    }
    total.sum_us = total
        .sum_us
        .checked_add(value.sum_us)
        .ok_or("延迟总和溢出")?;
    total.overflow |= value.overflow;
    Ok(())
}

/// 将互斥桶转换为 Prometheus 所需的累积桶。
fn append_histogram(
    output: &mut String,
    name: &str,
    buckets: [u64; 7],
    sum: u64,
    bounds: [&str; 7],
) -> Result<(), &'static str> {
    use std::fmt::Write as _;
    writeln!(output, "# TYPE {name} histogram").map_err(|_| "指标格式化失败")?;
    let mut cumulative = 0_u64;
    for (index, value) in buckets.into_iter().enumerate() {
        cumulative = cumulative.checked_add(value).ok_or("延迟桶累计溢出")?;
        let limit = bounds[index];
        writeln!(output, "{name}_bucket{{le=\"{limit}\"}} {cumulative}")
            .map_err(|_| "指标格式化失败")?;
    }
    writeln!(output, "{name}_sum {sum}\n{name}_count {cumulative}").map_err(|_| "指标格式化失败")
}

#[cfg(test)]
mod tests {
    use super::*;
    use matching_domain::{ClusterId, ShardId};
    use std::time::Duration;

    #[test]
    fn health_must_cover_exact_gateway_workers() {
        let mut a = crate::Shard::start(
            ShardId(901),
            ClusterId(901),
            crate::RouteTable::new([]).unwrap(),
            vec![],
        )
        .unwrap();
        let mut b = crate::Shard::start(
            ShardId(902),
            ClusterId(901),
            crate::RouteTable::new([]).unwrap(),
            vec![],
        )
        .unwrap();
        let ah = a.handle();
        let bh = b.handle();
        let health = Health::new(vec![ah.clone()]).unwrap();
        assert!(health.covers(std::slice::from_ref(&ah)));
        assert!(!health.covers(std::slice::from_ref(&bh)));
        assert!(!health.covers(&[ah.clone(), bh]));
        assert!(Health::new(vec![ah.clone(), ah]).is_err());
        a.shutdown(Duration::from_secs(1)).unwrap();
        b.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn unhealthy_transition_is_serialized_with_admission() {
        use std::sync::{Arc, mpsc};
        let mut shard = crate::Shard::start(
            ShardId(903),
            ClusterId(903),
            crate::RouteTable::new([]).unwrap(),
            vec![],
        )
        .unwrap();
        let health = Arc::new(Health::new(vec![shard.handle()]).unwrap());
        for dependency in [
            HealthDependency::Recovery,
            HealthDependency::Persistence,
            HealthDependency::Control,
        ] {
            health.set_healthy(dependency, true);
        }
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (resume_tx, resume_rx) = mpsc::sync_channel(1);
        let h = Arc::clone(&health);
        let admission = std::thread::spawn(move || {
            h.admit(|| {
                entered_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
                7
            })
        });
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let h = Arc::clone(&health);
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let failure = std::thread::spawn(move || {
            h.set_healthy(HealthDependency::Persistence, false);
            done_tx.send(()).unwrap();
        });
        assert!(done_rx.recv_timeout(Duration::from_millis(20)).is_err());
        resume_tx.send(()).unwrap();
        assert_eq!(admission.join().unwrap(), Some(7));
        done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        failure.join().unwrap();
        assert_eq!(health.admit(|| 8), None);
        shard.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn poisoned_admission_never_reports_ready() {
        use std::sync::Arc;
        let mut shard = crate::Shard::start(
            ShardId(904),
            ClusterId(904),
            crate::RouteTable::new([]).unwrap(),
            vec![],
        )
        .unwrap();
        let health = Arc::new(Health::new(vec![shard.handle()]).unwrap());
        for dep in [
            HealthDependency::Recovery,
            HealthDependency::Persistence,
            HealthDependency::Control,
        ] {
            health.set_healthy(dep, true);
        }
        let h = Arc::clone(&health);
        assert!(
            std::thread::spawn(move || h.admit(|| panic!("模拟准入异常")))
                .join()
                .is_err()
        );
        assert!(!health.ready());
        assert_eq!(health.admit(|| 1), None);
        shard.shutdown(Duration::from_secs(1)).unwrap();
    }
}
