//! 已加载费率版本的确定性保存和查询。
use crate::{AssetId, RejectReason};
use std::collections::BTreeMap;

/// 实际费用及返佣按明确配置的方向取整。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeeRounding {
    /// 向负无穷方向取整。
    Floor,
    /// 向正无穷方向取整。
    Ceiling,
    /// 向零方向取整。
    TowardZero,
}

/// 费用使用的资产类别。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeeAssetPolicy {
    /// 基础资产。
    Base,
    /// 报价资产。
    Quote,
    /// 独立费用资产。
    Third(
        /// 费用资产身份。
        AssetId,
    ),
    /// 第三资产与指定报价资产原子单位的精确、版本化换算比例。
    ThirdPriced {
        /// 费用资产身份。
        asset: AssetId,
        /// 换算基准对应的报价资产身份。
        quote_asset: AssetId,
        /// 每组报价原子单位对应的费用资产原子数。
        numerator: u128,
        /// 一组报价资产原子数。
        denominator: u128,
    },
}

/// 单个费率层级；负数代表返佣，单位为基点。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeeTier {
    /// 挂单方费率。
    pub maker_bps: i32,
    /// 吃单方费率。
    pub taker_bps: i32,
    /// 本版本费用取整方向。
    pub rounding: FeeRounding,
}

/// 不可变费率版本及按层级身份排序的配置。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeeSchedule {
    /// 稳定版本号。
    version: u64,
    /// 本版本费用资产策略。
    asset_policy: FeeAssetPolicy,
    /// 按身份排序的费率层级。
    tiers: BTreeMap<u32, FeeTier>,
}
impl FeeSchedule {
    /// 返回本配置的版本号。
    pub const fn version(&self) -> u64 {
        self.version
    }
    /// 返回本版本的费用资产策略。
    pub const fn asset_policy(&self) -> FeeAssetPolicy {
        self.asset_policy
    }
    /// 构造非空版本，拒绝重复层级。
    pub fn new(
        version: u64,
        asset_policy: FeeAssetPolicy,
        tiers: impl IntoIterator<Item = (u32, FeeTier)>,
    ) -> Result<Self, RejectReason> {
        let mut ordered = BTreeMap::new();
        for (id, tier) in tiers {
            if ordered.insert(id, tier).is_some() {
                return Err(RejectReason::FeeScheduleMismatch);
            }
        }
        if ordered.is_empty()
            || matches!(
                asset_policy,
                FeeAssetPolicy::ThirdPriced { numerator: 0, .. }
            )
            || matches!(
                asset_policy,
                FeeAssetPolicy::ThirdPriced { denominator: 0, .. }
            )
            || matches!(asset_policy, FeeAssetPolicy::ThirdPriced { asset, quote_asset, .. } if asset == quote_asset)
        {
            return Err(RejectReason::FeeScheduleMismatch);
        }
        Ok(Self {
            version,
            asset_policy,
            tiers: ordered,
        })
    }
}

/// 保留所有已接受版本的本地费率簿。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeeScheduleBook {
    /// 当前生效版本。
    active_version: u64,
    /// 所有版本按版本号排序；当前不回收历史版本。
    versions: BTreeMap<u64, FeeSchedule>,
}
impl FeeScheduleBook {
    /// 从升序历史和更高的生效版本构造完整初始费率簿。
    pub fn from_history(
        history: Vec<FeeSchedule>,
        schedule: FeeSchedule,
    ) -> Result<Self, RejectReason> {
        let mut ordered = history.into_iter();
        let Some(first) = ordered.next() else {
            return Ok(Self::new(schedule));
        };
        let mut book = Self::new(first);
        for entry in ordered {
            book.activate(entry)?;
        }
        book.activate(schedule)?;
        Ok(book)
    }
    /// 按版本顺序拆出历史及当前生效配置。
    pub fn into_history(self) -> (Vec<FeeSchedule>, FeeSchedule) {
        let mut values: Vec<_> = self.versions.into_values().collect();
        let active = values.pop().expect("有效费率簿必须包含活动版本");
        (values, active)
    }
    /// 仅在簿恰好包含一个版本时取出该不可变配置。
    pub fn into_single_schedule(self) -> Option<FeeSchedule> {
        if self.versions.len() != 1 {
            return None;
        }
        self.versions.into_values().next()
    }
    /// 按锁定版本获取不可变配置。
    pub fn schedule(&self, version: u64) -> Result<&FeeSchedule, RejectReason> {
        self.versions
            .get(&version)
            .ok_or(RejectReason::FeeScheduleNotFound)
    }
    /// 将完整费率簿编码为有界快照载荷，失败时不改变外层输出。
    pub fn write_snapshot(
        &self,
        writer: &mut crate::snapshot_wire::SnapshotEncoder,
    ) -> Result<(), crate::snapshot_wire::SnapshotWireError> {
        use crate::snapshot_wire::{SnapshotEncoder, SnapshotWireError};
        let mut body = SnapshotEncoder::new(256 * 1024 * 1024);
        body.u8(1)?;
        body.u64(self.active_version)?;
        body.u32(u32::try_from(self.versions.len()).map_err(|_| SnapshotWireError)?)?;
        for schedule in self.versions.values() {
            body.u64(schedule.version)?;
            match schedule.asset_policy {
                FeeAssetPolicy::Base => body.u8(0)?,
                FeeAssetPolicy::Quote => body.u8(1)?,
                FeeAssetPolicy::Third(asset) => {
                    body.u8(2)?;
                    body.u32(asset.get())?;
                }
                FeeAssetPolicy::ThirdPriced {
                    asset,
                    quote_asset,
                    numerator,
                    denominator,
                } => {
                    body.u8(3)?;
                    body.u32(asset.get())?;
                    body.u32(quote_asset.get())?;
                    body.u128(numerator)?;
                    body.u128(denominator)?;
                }
            }
            body.u32(u32::try_from(schedule.tiers.len()).map_err(|_| SnapshotWireError)?)?;
            for (id, tier) in &schedule.tiers {
                body.u32(*id)?;
                body.i64(i64::from(tier.maker_bps))?;
                body.i64(i64::from(tier.taker_bps))?;
                body.u8(match tier.rounding {
                    FeeRounding::Floor => 0,
                    FeeRounding::Ceiling => 1,
                    FeeRounding::TowardZero => 2,
                })?;
            }
        }
        writer.bytes(&body.finish())
    }

    /// 恢复快照并验证版本、层级顺序和调用方提供的数量上限。
    pub fn read_snapshot(
        reader: &mut crate::snapshot_wire::SnapshotDecoder<'_>,
        max_versions: usize,
        max_tiers: usize,
    ) -> Result<Self, crate::snapshot_wire::SnapshotWireError> {
        use crate::snapshot_wire::{SnapshotDecoder, SnapshotWireError};
        let bytes = reader.bytes()?;
        let mut body = SnapshotDecoder::new(bytes, 256 * 1024 * 1024)?;
        if body.u8()? != 1 {
            return Err(SnapshotWireError);
        }
        let active = body.u64()?;
        let count = usize::try_from(body.u32()?).map_err(|_| SnapshotWireError)?;
        if count == 0 || count > max_versions || count > body.remaining() / 13 {
            return Err(SnapshotWireError);
        }
        let mut versions = BTreeMap::new();
        let mut previous = None;
        for _ in 0..count {
            let version = body.u64()?;
            if previous.is_some_and(|old| old >= version) {
                return Err(SnapshotWireError);
            }
            previous = Some(version);
            let asset_policy = match body.u8()? {
                0 => FeeAssetPolicy::Base,
                1 => FeeAssetPolicy::Quote,
                2 => FeeAssetPolicy::Third(AssetId::new(body.u32()?)),
                3 => {
                    let asset = AssetId::new(body.u32()?);
                    let quote_asset = AssetId::new(body.u32()?);
                    let numerator = body.u128()?;
                    let denominator = body.u128()?;
                    if asset == quote_asset || numerator == 0 || denominator == 0 {
                        return Err(SnapshotWireError);
                    }
                    FeeAssetPolicy::ThirdPriced {
                        asset,
                        quote_asset,
                        numerator,
                        denominator,
                    }
                }
                _ => return Err(SnapshotWireError),
            };
            let tiers_count = usize::try_from(body.u32()?).map_err(|_| SnapshotWireError)?;
            if tiers_count == 0 || tiers_count > max_tiers || tiers_count > body.remaining() / 21 {
                return Err(SnapshotWireError);
            }
            let mut tiers = BTreeMap::new();
            let mut previous_tier = None;
            for _ in 0..tiers_count {
                let id = body.u32()?;
                if previous_tier.is_some_and(|old| old >= id) {
                    return Err(SnapshotWireError);
                }
                previous_tier = Some(id);
                let maker_bps = i32::try_from(body.i64()?).map_err(|_| SnapshotWireError)?;
                let taker_bps = i32::try_from(body.i64()?).map_err(|_| SnapshotWireError)?;
                let rounding = match body.u8()? {
                    0 => FeeRounding::Floor,
                    1 => FeeRounding::Ceiling,
                    2 => FeeRounding::TowardZero,
                    _ => return Err(SnapshotWireError),
                };
                tiers.insert(
                    id,
                    FeeTier {
                        maker_bps,
                        taker_bps,
                        rounding,
                    },
                );
            }
            versions.insert(
                version,
                FeeSchedule {
                    version,
                    asset_policy,
                    tiers,
                },
            );
        }
        body.finish()?;
        if previous != Some(active) {
            return Err(SnapshotWireError);
        }
        Ok(Self {
            active_version: active,
            versions,
        })
    }
    /// 从一个经过验证的初始版本建立费率簿。
    pub fn new(schedule: FeeSchedule) -> Self {
        Self {
            active_version: schedule.version,
            versions: BTreeMap::from([(schedule.version, schedule)]),
        }
    }
    /// 新版本必须严格递增；失败不改变当前版本及历史。
    pub fn activate(&mut self, schedule: FeeSchedule) -> Result<(), RejectReason> {
        if schedule.version <= self.active_version || self.versions.contains_key(&schedule.version)
        {
            return Err(RejectReason::FeeScheduleMismatch);
        }
        self.active_version = schedule.version;
        self.versions.insert(schedule.version, schedule);
        Ok(())
    }
    /// 返回当前生效版本号。
    pub const fn active_version(&self) -> u64 {
        self.active_version
    }
    /// 按锁定的版本及层级读取费率。
    pub fn resolve(&self, version: u64, tier: u32) -> Result<&FeeTier, RejectReason> {
        self.versions
            .get(&version)
            .ok_or(RejectReason::FeeScheduleNotFound)?
            .tiers
            .get(&tier)
            .ok_or(RejectReason::FeeScheduleMismatch)
    }
}
