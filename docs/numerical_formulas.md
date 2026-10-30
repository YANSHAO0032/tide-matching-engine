# 数值计算公式

本文说明整数价格、数量与 quote 金额之间的换算，以及预算、取整、市价保护和限价执行约束。

## 市场内优先级高水位

设一个市场的下一常规挂单优先级为 $`k_q`$，下一未触发止损优先级为 $`k_s`$，真实安全边界为 $`H_r`$，可选测试边界为 $`H_t`$。两类优先级使用独立计数器，生效边界为：

```math
H=\min(H_r,H_t).
```

当没有测试边界时取 $`H=H_r`$。只有下列条件成立时才能分配对应计数器的当前值：

```math
k_q\lt H,\qquad k_s\lt H.
```

成功后分别以 checked 加法推进：

```math
k_q'=k_q+1,\qquad k_s'=k_s+1.
```

当任一计数器达到 $`H`$，相应分配失败并保持全部状态不变。未触发止损只消耗 $`k_s`$；常规挂单、冰山补量及后续实际挂单的 child 才消耗 $`k_q`$。

## 原始整数与 scale

设价格、数量和 quote 金额的原始整数分别为 $`p`$、$`q`$、$`a`$，其 scale 分别为 $`s_p`$、$`s_q`$、$`s_a`$。对应的实际经济数值为：

```math
P = \frac{p}{10^{s_p}}, \qquad
Q = \frac{q}{10^{s_q}}, \qquad
A = \frac{a}{10^{s_a}}.
```

其中 $`p\gt 0`$、$`q\gt 0`$，而 $`a`$ 可以为零。价格以 quote/base 表示，数量以 base 表示，金额以 quote 表示。

原始价格、数量与非负 quote 金额分别使用有符号 64 位、无符号 64 位与无符号 128 位整数，其合法域为：

```math
1\leq p\leq 2^{63}-1,\qquad
1\leq q\leq 2^{64}-1,\qquad
0\leq a\leq 2^{128}-1.
```

初始 quote 预算必须严格正；成交累计金额和运行中的剩余预算可以为零。正值构造不代表已经通过市场 tick、lot 对齐检查。

## 精确成交本金

成交的 quote 原始金额必须满足：

```math
a = \frac{p \cdot q \cdot 10^{s_a}}{10^{s_p+s_q}}.
```

令：

```math
\Delta = s_a-s_p-s_q.
```

当 $`\Delta\lt 0`$ 时，先验证 $`p\cdot q`$ 能被 $`10^{-\Delta}`$ 整除，再精确相除；当 $`\Delta\geq0`$ 时，使用 checked 整数乘法乘以 $`10^{\Delta}`$。本金不允许 floor、ceil 或任何四舍五入。

上述比例式表达数学关系，不要求分别展开分子和分母中的大幂。配置先约去公共幂，仅计算 $`10^{|\Delta|}`$；该因子必须能以无符号 128 位整数表示，否则拒绝配置。精确可整除不保证最终金额可表示，单笔本金和累计本金仍须逐次检查上界。

## 最小可成交单位

设 tick 原始步长为 $`t\gt 0`$，lot 原始步长为 $`l\gt 0`$。最小成交本金 $`K`$ 为：

```math
K = \frac{t \cdot l \cdot 10^{s_a}}{10^{s_p+s_q}}.
```

配置只有在 $`K`$ 是严格正且可表示的整数时才能生效。对任何合法价格和数量：

```math
p=N\cdot t, \qquad q=M\cdot l,
```

其中 $`N,M`$ 为正整数，则成交本金等价为：

```math
a=N\cdot M\cdot K.
```

实现先检查 price/tick 与 qty/lot 对齐，再以 checked 整数计算此式。

## Quote 预算反推

给定剩余 quote 预算 $`B_r`$ 和价格倍数 $`N=p/t`$，其中 $`B_r\geq0`$、$`N\geq1`$，预算可支付的完整 lot 个数为：

```math
L_{\mathrm{budget}}=\left\lfloor\frac{\left\lfloor B_r/K\right\rfloor}{N}\right\rfloor.
```

令 $`Q_{\mathrm{cap}}=2^{64}-1`$ 为数量原始整数的表示上限。可表示的完整 lot 上限与最终 lot 个数为：

```math
L_{\mathrm{cap}}=\left\lfloor\frac{Q_{\mathrm{cap}}}{l}\right\rfloor,
\qquad
L_{\max}=\min\left(L_{\mathrm{budget}},L_{\mathrm{cap}}\right).
```

最大合法原始数量为：

```math
q_{\max}=L_{\max}\cdot l.
```

若 $`L_{\max}=0`$，返回无可执行数量并终止当前循环，不构造零数量成交。否则返回严格正、lot 对齐且本金不超过预算的数量。数量表示上限不代表预算已经耗尽；实际成交量还受 maker 可见余量限制。

## 预算守恒与费用边界

对初始预算 $`B`$、成交本金总和 $`S`$ 和未使用预算 $`U`$：

```math
S=\sum_i a_i, \qquad U=B-S, \qquad S+U=B, \qquad 0\leq S\leq B.
```

费用的取整策略独立于本金计算。本金必须精确；费用的向上、向下或其他取整只能由冻结的费率策略明确规定。

## 限价执行量与余量守恒

设入场限价为 $`p_L`$，提交数量原始值为 $`q_0\gt0`$。对手订单按最优价格优先、同价 FIFO 排列；第 $`i`$ 个可执行 maker 的价格与可见剩余量分别为 $`p_i`$、$`v_i`$。以下关系以价格和数量已通过 tick/lot 校验为前提，maker 可见余量严格正。限价内的价格条件为：

```math
p_i\leq p_L\quad\mathrm{(Buy)},\qquad
p_i\geq p_L\quad\mathrm{(Sell)}.
```

只扫描满足价格条件的 maker 前缀。设扫描前余量为 $`r_0=q_0`$，每笔计划成交量与新余量为：

```math
f_i=\min(r_{i-1},v_i),\qquad
r_i=r_{i-1}-f_i.
```

余量为零时停止，不再构造零数量成交；无对手或下一 maker 超出限价时也停止。设共计划 $`n`$ 笔，未成交余量为 $`R`$，则：

```math
F=\sum_{i=1}^{n}f_i,\qquad R=r_n,\qquad
F+R=q_0,\qquad 0\leq F\leq q_0.
```

空计划的和为零。每笔成交使用 maker 价格，并复用精确本金换算；IOC/FOK 在任何改簿前检查完整提交价格与数量的合法性、每笔本金及其累计值。仅检查限价处的整单本金并不足够：卖方在更高 maker 价格成交时，逐笔或累计金额仍可能越界。

IOC 提交计划中的全部成交，并取消正余量 $`R`$；余量不入簿。FOK 只有在 $`R=0`$ 且其他预检全部通过时才提交，否则实际成交量为零，簿状态保持不变。预检计划中的 $`F`$ 不等于已经发生的成交。两者的 incoming 不分配优先级；成交期间冰山 maker 的补量仍需要新的优先级，且必须在预检中计入。

## 冰山可见量、隐藏量与补量

令原始总下单量为 $`Q`$，每次显示上限为 $`K`$，输入必须满足：

```math
0\lt K\lt Q\leq 2^{64}-1.
```

总量和显示上限分别满足市场 lot 对齐；本金校验使用总量，不能只校验显示量。输入订单先按总量与限价尝试成交；若还有总余量 $`T\gt0`$，挂单时分为可见量 $`v`$ 与隐藏量 $`h`$：

```math
v=\min(K,T),\qquad h=T-v,\qquad T=v+h.
```

剩余量可能小于显示上限，此时整笔余量可见。对已挂单的 maker，单笔成交只能消耗可见量。设 incoming 剩余需求为 $`r`$：

```math
f=\min(r,v),\qquad
v'=v-f,\qquad h'=h,\qquad T'=T-f=v'+h'.
```

当 $`v'=0`$ 且 $`h'\gt0`$ 时，立即补量；即使本次成交已经用完 incoming，也不能把可见量为零的订单留在簿中：

```math
a=\min(K,h'),\qquad
v''=a,\qquad h''=h'-a,\qquad
v''+h''=T'.
```

补量不增加或消灭总余量，不是成交。补量后 maker 移到同侧同价队尾，并取得新的队列优先级；下一笔成交必须重新选择队首，不能连续跳过其他 maker 消耗该订单的隐藏量。FOK 的预检使用相同的补量顺序，并计入所有可执行片段。

设成交前本档公开可见深度为 $`D_v`$；没有补量时取 $`a=0`$，该次成交及可选补量后的深度为：

```math
D_v'=D_v-f+a,\qquad 0\leq D_v'\leq 2^{64}-1.
```

补量可能比刚成交的片段大，因此聚合可能增长；必须 checked 检查减法与加法，不能假定成交只会降低深度。深度只累计可见量，隐藏量不加入公开档位聚合。部分成交事件报告 $`T'`$；取消移除的总余量为 $`v+h`$，而公开深度只减少 $`v`$。

## 挂单修改的绝对数量与显示暴露

令当前保存的总目标为 $`Q`$，当前总余量为 $`T`$，已成交量为 $`F`$，新的绝对总目标为 $`Q_a`$：

```math
F=Q-T,\qquad Q_a\gt F,\qquad T_a=Q_a-F.
```

没有新总目标时保留原总目标；所有差值使用带溢出检查的整数运算。总目标等于已成交量时不产生零量挂单，应使用取消操作。

对于冰山，原峰值为 $`K`$，新峰值为 $`K_a`$，原可见片为 $`v`$。同价且总量和峰值均不增加时保留优先级，并只裁剪现有显示片：

```math
T_a\leq T,\qquad K_a\leq K,\qquad
v_a=\min(v,K_a,T_a),\qquad h_a=T_a-v_a,\qquad T_a=v_a+h_a.
```

峰值必须严格小于新的总目标，而不是当前余量。该减量路径不把隐藏量提前公开；减少峰值可能将原显示片的一部分重新隐藏。增加总量、增加峰值或改变价格均失去优先级；改价先按全部候选余量撮合，只有实际余量挂单时取得新优先级，显示量继续使用挂单时的峰值与余量取较小者。时效改变本身不改变优先级。

## 只挂单交叉条件

设最优卖价为 $`p_{\mathrm{ask}}`$，最优买价为 $`p_{\mathrm{bid}}`$。当对应对手侧存在订单时，PostOnly 拒绝条件为：

```math
p_L\geq p_{\mathrm{ask}}\quad\mathrm{(Buy)},\qquad
p_L\leq p_{\mathrm{bid}}\quad\mathrm{(Sell)}.
```

等价价格也会交叉；对手侧为空时不存在交叉。通过全部挂单预检后，订单进入本方价档尾部，并分配一次队列优先级；普通限价量全部可见，冰山按显示上限分配可见与隐藏量。任何拒绝均不改变簿与优先级。成功挂单后允许作为后续成交的 maker。

## 价档聚合与队列优先级

同侧同价档位中的可见剩余量以 $`v_j`$ 表示，档位聚合量为：

```math
V=\sum_{j=1}^{c}v_j,\qquad 0\leq V\leq 2^{64}-1.
```

其中 $`c`$ 是该档位的活跃订单数，空档聚合为零。新挂单前以当前可见余量检查聚合上界，不能用订单原始数量或隐藏量替代；部分成交减少可见量，冰山补量增加可见量，总余量为零或取消才移除订单。

设队列优先级的下一待分配值为 $`k`$，保留安全区为 $`g`$，可选的更早耗尽边界为 $`H_s`$。真实边界与生效边界为：

```math
H_r=(2^{128}-1)-g,\qquad
H=\min(H_r,H_s).
```

缺少更早边界时直接使用 $`H=H_r`$；策略构造保证安全区严格正且生效边界非零。仅当 $`k\lt H`$ 时可以分配：返回 $`k`$，再以 checked 加法推进为 $`k+1`$。$`k=H`$ 表示合法但已耗尽的状态，不能再分配；失败保持原值。订单簿只在实际成功挂单或冰山补量重新排队时提交这次推进；整条命令预检计入每次所需分配，任一次不可分配均在真实改簿前拒绝。

## 市价保护边界

设命令开始时固定的参考价格原始值为 $`r\gt 0`$，tick 原始步长为 $`t\gt 0`$，滑点或价格带偏离参数为 $`b`$，并令基点分母为：

```math
D=10\,000, \qquad 0\leq b\leq 9\,999.
```

参考价格、保护边界和 maker 价格使用同一原始价格整数空间。参考价格可以不对齐 tick；计算出的保护边界必须对齐。以 F 和 C 分别表示原始整数空间中向下、向上的 tick 对齐函数：

```math
F_t(x)=t\left\lfloor\frac{x}{t}\right\rfloor,
\qquad
C_t(x)=t\left\lceil\frac{x}{t}\right\rceil.
```

买方允许的最高价格为先向下取整、再向下对齐的结果：

```math
P_{\mathrm{bps,buy}}
=F_t\!\left(
\left\lfloor\frac{r(D+b)}{D}\right\rfloor
\right).
```

卖方允许的最低价格为先向上取整、再向上对齐的结果：

```math
P_{\mathrm{bps,sell}}
=C_t\!\left(
\left\lceil\frac{r(D-b)}{D}\right\rceil
\right).
```

市场价格带以相同计算得到上界 $`P_{\mathrm{band,upper}}=P_{\mathrm{bps,buy}}`$ 和下界 $`P_{\mathrm{band,lower}}=P_{\mathrm{bps,sell}}`$。限价必须处于包含两个端点的区间内；若下界大于上界，区间内没有合法 tick 价格。非法的滑点或价格带偏离基点统一以 `InvalidSlippageBps = 3007` 拒绝。

对非负整数 $`a`$ 与正整数 $`d`$，向上除法以商 $`q`$ 和余数 $`u`$ 表示：

```math
\left\lceil\frac{a}{d}\right\rceil=
\begin{cases}
q, & u=0,\\
q+1, & u\ne0,
\end{cases}
\qquad a=qd+u, \quad 0\leq u\lt d.
```

实现必须只在 $`u\ne0`$ 时执行 checked 的 $`q+1`$；不得用 $`\left(a+d-1\right)/d`$ 替代此式。所有中间乘法、加减法、取整增量、tick 对齐乘法和目标整数域转换都必须 checked。

计算结果还必须是严格正且可表示的价格。买方向下对齐可能得到零，卖方向上对齐或买方滑点扩张可能超过价格上限；这两类结果均以 `ArithmeticOverflow` 拒绝，不创建零价或截断后的价格。

显式用户最差价 $`P_{\mathrm{explicit}}`$ 必须已经满足 $`P_{\mathrm{explicit}}\bmod t=0`$；不满足时以 `InvalidTick` 拒绝，而不作方向性取整。若同时存在由 bps 得到的边界、显式用户最差价和市场 band 边界，则只保留对用户更严格的候选。只对存在的候选取 min/max；至少存在一个候选时：

```math
P_{\mathrm{effective,buy}}
=\min\!\left(P_{\mathrm{bps,buy}},P_{\mathrm{explicit,buy}},P_{\mathrm{band,upper}}\right),
```

```math
P_{\mathrm{effective,sell}}
=\max\!\left(P_{\mathrm{bps,sell}},P_{\mathrm{explicit,sell}},P_{\mathrm{band,lower}}\right).
```

所有候选均缺失时，没有有效边界，也没有保护来源；该结果本身不决定订单是否必须要求保护。

市场 band 候选由调用方预先计算和验证。合并候选仅执行方向性的 min/max，不代替 band 的构造或 tick 校验。

令 $`P_u`$ 表示已合并的用户最差价，$`P_b`$ 表示市场 band 边界。只有用户候选时来源为 `User`，只有市场候选时为 `MarketBand`。以下分段式仅适用于两个候选都存在的情况。买方的严格性与较小价格对应：

```math
S_{\mathrm{buy}}=
\begin{cases}
\mathrm{User}, & P_u\lt P_b,\\
\mathrm{MarketBand}, & P_b\lt P_u,\\
\mathrm{Both}, & P_u=P_b.
\end{cases}
```

卖方的严格性与较大价格对应：

```math
S_{\mathrm{sell}}=
\begin{cases}
\mathrm{User}, & P_u\gt P_b,\\
\mathrm{MarketBand}, & P_b\gt P_u,\\
\mathrm{Both}, & P_u=P_b.
\end{cases}
```

边界为包含式：买方 maker 价格 $`p_m\leq P_{\mathrm{effective,buy}}`$ 可以成交，卖方 $`p_m\geq P_{\mathrm{effective,sell}}`$ 可以成交。首次越界的 maker 不产生成交。

例如 $`r=100\,003`$、$`t=10`$、$`b=25`$ 时：

```math
P_{\mathrm{bps,buy}}=100\,250,
\qquad
P_{\mathrm{bps,sell}}=99\,760.
```

## 止损价格跨越与来源序号

设同一参考来源前一次已接受的价格为 $`r_0`$，本次严格递增来源序号携带的价格为 $`r_1`$，止损价为 $`p_s`$。买方止损只在参考价格上升且跨越止损价时收集：

```math
r_0\lt p_s\leq r_1.
```

卖方止损只在参考价格下降且跨越止损价时收集：

```math
r_1\leq p_s\lt r_0.
```

首次接受某一来源只建立基线，不推断跨越。对该来源已保存的序号 $`s_0`$，新序号 $`s_1`$ 必须满足：

```math
s_1\gt s_0.
```

买方按 $`p_s`$ 升序、卖方按 $`p_s`$ 降序处理；同一止损价按原 StopPriority 先进先出。被收集的订单会先离开止损容器和到期索引，随后才进入 child 预检。child 成交更新最近成交价时，内部序号以 checked 加法推进；不能表示下一序号时在首笔 child 成交前拒绝该根处理。
