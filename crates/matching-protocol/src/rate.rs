//! 有界网关准入速率限制；输入时间不进入撮合业务。

/// 每个桶的令牌容量和补充一个令牌所需的时间间隔。
#[derive(Clone, Copy, Debug)]
pub struct BucketConfig {
    /// 桶中的最大令牌数，必须大于零。
    pub capacity: u64,
    /// 相邻令牌补充间隔，必须大于零。
    pub refill_interval: u64,
}

/// 网关速率检查失败。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RateError {
    /// 桶配置无效。
    InvalidConfig,
    /// 至少一个桶缺少令牌。
    Limited,
    /// 身份表达到容量上限或内存分配失败。
    Capacity,
    /// 输入的单调时间倒退。
    ClockRegression,
}

#[derive(Clone, Copy)]
struct Bucket {
    /// 上次成功准入后的令牌余额。
    tokens: u64,
    /// 补充时间锚点，单位由调用方统一指定。
    at: u64,
}
impl Bucket {
    fn take(self, config: BucketConfig, now: u64) -> Result<Self, RateError> {
        let elapsed = now.checked_sub(self.at).ok_or(RateError::ClockRegression)?;
        let added = elapsed / config.refill_interval;
        let room = config
            .capacity
            .checked_sub(self.tokens)
            .ok_or(RateError::InvalidConfig)?;
        let tokens = self
            .tokens
            .checked_add(added.min(room))
            .ok_or(RateError::InvalidConfig)?;
        if tokens == 0 {
            return Err(RateError::Limited);
        }
        let at = if tokens == config.capacity {
            now
        } else {
            now.checked_sub(elapsed % config.refill_interval)
                .ok_or(RateError::ClockRegression)?
        };
        Ok(Self {
            tokens: tokens - 1,
            at,
        })
    }
}

/// 全局、API key 和用户三层桶；所有桶仅在整体准入成功后扣减。
pub struct RateLimiter {
    /// 全局、密钥和用户桶的配置。
    configs: [BucketConfig; 3],
    /// 两类身份表分别使用的容量上限。
    limit: usize,
    /// 全局桶的最近已提交状态。
    global: Option<Bucket>,
    /// 有界密钥桶及身份。
    keys: Vec<(u64, Bucket)>,
    /// 有界用户桶及身份。
    users: Vec<(u64, Bucket)>,
    /// 最近一次成功准入的单调时间。
    last: Option<u64>,
}

/// 两组独立、均按全局、密钥和用户计费的网关限流器。
///
/// 所有会话须共享同一实例；准备写入和分片入队分别受限，不能囤积准入额度。
pub struct GatewayRateLimiters {
    /// 限制意图身份持久登记的令牌桶。
    prepare: RateLimiter,
    /// 限制每次分片入队尝试的令牌桶。
    submit: RateLimiter,
}
impl GatewayRateLimiters {
    /// 以相同配置创建两组独立令牌桶。
    pub fn new(
        global: BucketConfig,
        key: BucketConfig,
        user: BucketConfig,
        limit: usize,
    ) -> Result<Self, RateError> {
        Ok(Self {
            prepare: RateLimiter::new(global, key, user, limit)?,
            submit: RateLimiter::new(global, key, user, limit)?,
        })
    }
    /// 检查持久准备阶段。
    pub fn check_prepare(&mut self, key: u64, user: u64, now: u64) -> Result<(), RateError> {
        self.prepare.check(key, user, now)
    }
    /// 检查每次分片入队尝试。
    pub fn check_submit(&mut self, key: u64, user: u64, now: u64) -> Result<(), RateError> {
        self.submit.check(key, user, now)
    }
}
impl RateLimiter {
    /// 构造有限身份表；达到上限后新身份被拒绝，不自动淘汰已有桶。
    pub fn new(
        global: BucketConfig,
        key: BucketConfig,
        user: BucketConfig,
        limit: usize,
    ) -> Result<Self, RateError> {
        let configs = [global, key, user];
        if limit == 0
            || configs
                .iter()
                .any(|c| c.capacity == 0 || c.refill_interval == 0)
        {
            return Err(RateError::InvalidConfig);
        }
        Ok(Self {
            configs,
            limit,
            global: None,
            keys: Vec::new(),
            users: Vec::new(),
            last: None,
        })
    }

    /// 原子检查三层并消耗令牌；时间由网关单调时钟提供，单位与配置一致。
    pub fn check(&mut self, key: u64, user: u64, now: u64) -> Result<(), RateError> {
        if self.last.is_some_and(|last| now < last) {
            return Err(RateError::ClockRegression);
        }
        let ki = self.keys.iter().position(|(id, _)| *id == key);
        let ui = self.users.iter().position(|(id, _)| *id == user);
        if (ki.is_none() && self.keys.len() == self.limit)
            || (ui.is_none() && self.users.len() == self.limit)
        {
            return Err(RateError::Capacity);
        }
        let fresh = |c: BucketConfig| Bucket {
            tokens: c.capacity,
            at: now,
        };
        let g = self
            .global
            .unwrap_or_else(|| fresh(self.configs[0]))
            .take(self.configs[0], now)?;
        let k = ki
            .map_or_else(|| fresh(self.configs[1]), |i| self.keys[i].1)
            .take(self.configs[1], now)?;
        let u = ui
            .map_or_else(|| fresh(self.configs[2]), |i| self.users[i].1)
            .take(self.configs[2], now)?;
        if ki.is_none() {
            self.keys.try_reserve(1).map_err(|_| RateError::Capacity)?;
        }
        if ui.is_none() {
            self.users.try_reserve(1).map_err(|_| RateError::Capacity)?;
        }
        self.global = Some(g);
        if let Some(i) = ki {
            self.keys[i].1 = k;
        } else {
            self.keys.push((key, k));
        }
        if let Some(i) = ui {
            self.users[i].1 = u;
        } else {
            self.users.push((user, u));
        }
        self.last = Some(now);
        Ok(())
    }
}
