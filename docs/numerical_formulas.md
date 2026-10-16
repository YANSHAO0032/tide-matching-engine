# 数值计算公式

本文件是撮合核心的数值公式权威说明。涉及价格、数量、名义额、预算反推、费用或结算的计算关系时，先在此处补充或修订公式，再同步规格、实现和测试。公式使用 TeX 表示：行内公式使用 `$...$`，独立公式使用 `$$...$$`；不得在本文件用阶段或任务编号表达职责边界。

## 原始整数与 scale

设价格、数量和 quote 金额的原始整数分别为 $p$、$q$、$a$，其 scale 分别为 $s_p$、$s_q$、$s_a$。对应的实际经济数值为：

$$
P = \frac{p}{10^{s_p}}, \qquad
Q = \frac{q}{10^{s_q}}, \qquad
A = \frac{a}{10^{s_a}}.
$$

其中 $p>0$、$q>0$，而 $a$ 可以为零。价格以 quote/base 表示，数量以 base 表示，金额以 quote 表示。

## 精确成交本金

成交的 quote 原始金额必须满足：

$$
a = \frac{p \cdot q \cdot 10^{s_a}}{10^{s_p+s_q}}.
$$

令：

$$
\Delta = s_a-s_p-s_q.
$$

当 $\Delta<0$ 时，先验证 $p\cdot q$ 能被 $10^{-\Delta}$ 整除，再精确相除；当 $\Delta\geq0$ 时，使用 checked 整数乘法乘以 $10^{\Delta}$。本金不允许 floor、ceil 或任何四舍五入。

## 最小可成交单位

设 tick 原始步长为 $t>0$，lot 原始步长为 $l>0$。最小成交本金 $K$ 为：

$$
K = \frac{t \cdot l \cdot 10^{s_a}}{10^{s_p+s_q}}.
$$

配置只有在 $K$ 是严格正且可表示的整数时才能生效。对任何合法价格和数量：

$$
p=N\cdot t, \qquad q=M\cdot l,
$$

其中 $N,M$ 为正整数，则成交本金等价为：

$$
a=N\cdot M\cdot K.
$$

实现先检查 price/tick 与 qty/lot 对齐，再以 checked 整数计算此式。

## Quote 预算反推

给定剩余 quote 预算 $b$ 和价格倍数 $N=p/t$，可支付的最大 lot 个数为：

$$
L_{\max}=\left\lfloor\frac{\left\lfloor b/K\right\rfloor}{N}\right\rfloor.
$$

原始数量为：

$$
q_{\max}=L_{\max}\cdot l.
$$

还必须受数量表示域限制。若 $L_{\max}=0$，没有可执行 lot，应终止当前循环；不得构造零数量成交。

## 预算守恒与费用边界

对初始预算 $B$、成交本金总和 $S$ 和未使用预算 $U$：

$$
S=\sum_i a_i, \qquad U=B-S, \qquad S+U=B, \qquad 0\leq S\leq B.
$$

费用的取整策略独立于本金计算。本金必须精确；费用的向上、向下或其他取整只能由冻结的费率策略明确规定。

## 市价保护边界

设命令开始时固定的参考价格原始值为 $r>0$，tick 原始步长为 $t>0$，滑点参数为 $b$，并令基点分母为：

$$
D=10\,000, \qquad 0\leq b\leq 9\,999.
$$

参考价格、保护边界和 maker 价格使用同一原始价格整数空间。参考价格可以不对齐 tick；计算出的保护边界必须对齐。定义原始整数空间中的定向 tick 对齐：

$$
\operatorname{floor}_t(x)=t\left\lfloor\frac{x}{t}\right\rfloor,
\qquad
\operatorname{ceil}_t(x)=t\left\lceil\frac{x}{t}\right\rceil.
$$

买方允许的最高价格为先向下取整、再向下对齐的结果：

$$
P_{\mathrm{bps,buy}}
=\operatorname{floor}_t\!\left(
\left\lfloor\frac{r(D+b)}{D}\right\rfloor
\right).
$$

卖方允许的最低价格为先向上取整、再向上对齐的结果：

$$
P_{\mathrm{bps,sell}}
=\operatorname{ceil}_t\!\left(
\left\lceil\frac{r(D-b)}{D}\right\rceil
\right).
$$

向上除法以商 $q$ 和余数 $u$ 表示：

$$
\left\lceil\frac{a}{d}\right\rceil=
\begin{cases}
q, & u=0,\\
q+1, & u\ne0,
\end{cases}
\qquad a=qd+u, \quad 0\leq u<d.
$$

实现必须只在 $u\ne0$ 时执行 checked 的 $q+1$；不得用 $\left(a+d-1\right)/d$ 替代此式。所有中间乘法、加减法、取整增量、tick 对齐乘法和目标整数域转换都必须 checked。

显式用户最差价 $P_{\mathrm{explicit}}$ 必须已经满足 $P_{\mathrm{explicit}}\bmod t=0$；不满足时以 `InvalidTick` 拒绝，而不作方向性取整。若同时存在由 bps 得到的边界、显式用户最差价和市场 band 边界，则只保留对用户更严格的候选。设缺失候选不参与 min/max，则：

$$
P_{\mathrm{effective,buy}}
=\min\!\left(P_{\mathrm{bps,buy}},P_{\mathrm{explicit,buy}},P_{\mathrm{band,upper}}\right),
$$

$$
P_{\mathrm{effective,sell}}
=\max\!\left(P_{\mathrm{bps,sell}},P_{\mathrm{explicit,sell}},P_{\mathrm{band,lower}}\right).
$$

令 $P_u$ 表示已合并的用户最差价候选，$P_b$ 表示市场 band 候选。保护触发来源由实际决定有效边界的候选确定：只有一个候选时归属该候选；两个候选同样严格时归属 Both。买方的严格性与较小价格对应：

$$
S_{\mathrm{buy}}=
\begin{cases}
\mathrm{User}, & P_b=\varnothing\ \text{or}\ P_u<P_b,\\
\mathrm{MarketBand}, & P_u=\varnothing\ \text{or}\ P_b<P_u,\\
\mathrm{Both}, & P_u=P_b.
\end{cases}
$$

卖方的严格性与较大价格对应：

$$
S_{\mathrm{sell}}=
\begin{cases}
\mathrm{User}, & P_b=\varnothing\ \text{or}\ P_u>P_b,\\
\mathrm{MarketBand}, & P_u=\varnothing\ \text{or}\ P_b>P_u,\\
\mathrm{Both}, & P_u=P_b.
\end{cases}
$$

边界为包含式：买方 maker 价格 $p_m\leq P_{\mathrm{effective,buy}}$ 可以成交，卖方 $p_m\geq P_{\mathrm{effective,sell}}$ 可以成交。首次越界的 maker 不产生成交。

例如 $r=100\,003$、$t=10$、$b=25$ 时：

$$
P_{\mathrm{bps,buy}}=100\,250,
\qquad
P_{\mathrm{bps,sell}}=99\,760.
$$
