//! 仅用于验证和示例的确定性内存参考账本。
use crate::TradeSettlement;
use matching_domain::{AssetId, TradeId, UserId};
use std::collections::{BTreeMap, BTreeSet};

/// 参考账本拒绝交易的内部错误。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReferenceLedgerError {
    /// 资产或初始账户配置无效。
    InvalidConfiguration,
    /// 同一成交身份已处理。
    DuplicateTrade,
    /// 账户余额不足。
    InsufficientBalance,
    /// 金额或汇总不能表示。
    Overflow,
}

/// 不连接生产账户系统的内存参考账本。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReferenceLedger {
    /// 汇集费用与支付返佣的账户。
    fee_account: UserId,
    /// 按用户和资产确定性排序的余额。
    balances: BTreeMap<(UserId, AssetId), u128>,
    /// 已应用的成交身份。
    applied: BTreeSet<TradeId>,
}
impl ReferenceLedger {
    /// 建立参考余额，重复账户资产对视为无效输入。
    pub fn new(
        fee_account: UserId,
        balances: impl IntoIterator<Item = (UserId, AssetId, u128)>,
    ) -> Result<Self, ReferenceLedgerError> {
        let mut ordered = BTreeMap::new();
        for (user, asset, amount) in balances {
            if ordered.insert((user, asset), amount).is_some() {
                return Err(ReferenceLedgerError::InvalidConfiguration);
            }
        }
        Ok(Self {
            fee_account,
            balances: ordered,
            applied: BTreeSet::new(),
        })
    }
    /// 查询账户资产余额；未配置的余额为零。
    pub fn balance(&self, user: UserId, asset: AssetId) -> u128 {
        self.balances.get(&(user, asset)).copied().unwrap_or(0)
    }
    /// 在副本上聚合双边本金与费用，全部检查通过后一次安装。
    pub fn apply_trade(
        &mut self,
        base: AssetId,
        quote: AssetId,
        trade: &TradeSettlement,
    ) -> Result<(), ReferenceLedgerError> {
        if base == quote || base != trade.base_asset || quote != trade.quote_asset {
            return Err(ReferenceLedgerError::InvalidConfiguration);
        }
        self.apply_event(&trade.to_event_payload())
    }

    /// 以实际结算事件为输入，独立验证本金与费用账户的资产守恒。
    pub fn apply_event(
        &mut self,
        trade: &matching_domain::SettlementDelta,
    ) -> Result<(), ReferenceLedgerError> {
        let (base, quote) = (trade.base_asset, trade.quote_asset);
        if base == quote {
            return Err(ReferenceLedgerError::InvalidConfiguration);
        }
        if self.applied.contains(&trade.trade_id) {
            return Err(ReferenceLedgerError::DuplicateTrade);
        }
        let mut deltas = BTreeMap::<(UserId, AssetId), i128>::new();
        let mut add = |user, asset, amount: i128| -> Result<(), ReferenceLedgerError> {
            let current = deltas.entry((user, asset)).or_insert(0);
            *current = current
                .checked_add(amount)
                .ok_or(ReferenceLedgerError::Overflow)?;
            Ok(())
        };
        add(trade.maker_user, base, trade.base_delta.get())?;
        add(
            trade.taker_user,
            base,
            trade
                .base_delta
                .get()
                .checked_neg()
                .ok_or(ReferenceLedgerError::Overflow)?,
        )?;
        add(trade.maker_user, quote, trade.quote_delta.get())?;
        add(
            trade.taker_user,
            quote,
            trade
                .quote_delta
                .get()
                .checked_neg()
                .ok_or(ReferenceLedgerError::Overflow)?,
        )?;
        for (user, fee) in [
            (trade.maker_user, &trade.maker_fee),
            (trade.taker_user, &trade.taker_fee),
        ] {
            add(
                user,
                fee.asset,
                fee.amount
                    .get()
                    .checked_neg()
                    .ok_or(ReferenceLedgerError::Overflow)?,
            )?;
            add(self.fee_account, fee.asset, fee.amount.get())?;
        }
        let mut next = self.balances.clone();
        for (key, delta) in deltas {
            let old = next.get(&key).copied().unwrap_or(0);
            let value = old
                .checked_add_signed(delta)
                .ok_or(if delta.is_negative() {
                    ReferenceLedgerError::InsufficientBalance
                } else {
                    ReferenceLedgerError::Overflow
                })?;
            next.insert(key, value);
        }
        self.balances = next;
        self.applied.insert(trade.trade_id);
        Ok(())
    }
}
