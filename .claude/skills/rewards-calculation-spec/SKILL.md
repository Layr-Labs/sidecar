---
name: rewards-calculation-spec
description: EigenLayer reward calculation spec covering all 7 reward types, formulas, eligibility, snapshot timing, and arithmetic rules. Use when reviewing reward calculations, debugging distribution amounts, or verifying correctness.
---

# EigenLayer Reward Calculation Spec

All formulas use **integer floor division** (truncation toward zero).

## Definitions

| Symbol | Meaning |
|:-------|:--------|
| $t_s$ | `startTimestamp` of the reward submission |
| $D$ | `duration` of the reward submission (seconds) |
| $N$ | Number of reward days: $D / 86400$ |
| $R$ | Daily rate: $\lfloor \text{amount} / N \rfloor$ |
| $d$ | A snapshot day (midnight-aligned timestamp) |
| $S$ | Set of strategies in `strategiesAndMultipliers[]` |
| $m_s$ | Multiplier for strategy $s$ |
| $\text{opShares}_o(s)$ | Operator $o$'s delegated shares in strategy $s$ (slashing-adjusted) |
| $\text{shares}_i(s)$ | Staker $i$'s withdrawable shares in strategy $s$ (slashing-adjusted) |
| $\text{mag}_o(s)$ | Operator $o$'s allocated magnitude for strategy $s$ in the operator set |
| $\text{maxMag}_o(s)$ | Operator $o$'s max magnitude for strategy $s$ (starts at $10^{18}$) |
| $\alpha_o(s)$ | Allocation ratio: $\text{mag}_o(s) / \text{maxMag}_o(s)$ |
| $w_o$ | Operator weight (formula varies by type) |
| $w_i$ | Staker weight (formula varies by type) |
| $W$ | Total weight |
| $T_o$ | Total staker weight under operator $o$ |
| $p_i$ | Staker proportion (15-decimal fixed-point) |
| $\text{splitBips}$ | Operator split in basis points (0–10000) |

---

## Snapshot Day

All calculations run **per snapshot day**. A snapshot day $d$ is active for a reward submission if:

$$t_s < d \le t_s + D \qquad \text{(start-exclusive, end-inclusive)}$$

The **snapshot block** for day $d$ is the last block whose timestamp is $< d$ (final block before midnight UTC). All state reads (shares, magnitudes, delegation, registration) MUST reflect state at this block.

**Timing nuances:**

- **Shares, delegation, registration, splits:** Event on day $D$ → affects snapshots starting day $D+1$
- **Allocation increases:** Day $D$ → affects day $D+1$
- **Allocation decreases (slashing):** Day $D$ → affects day $D$ (same day)
- **Splits:** Use the `activated_at` timestamp (not transaction time). Activated on day $D$ → takes effect day $D+1$

**Cutoff rule:** Submission on day $D$ requires `cutoffDate >= D+1` to be visible.

---

## Shares

Both staker and operator shares MUST be **slashing-adjusted** (i.e., withdrawable shares, not raw deposit shares). When an operator is slashed, deposit shares are unchanged — only withdrawable shares reflect the reduction. Slashing takes effect immediately at the block the slash occurs.

---

## Split

The operator split is the percentage (in basis points) the operator keeps. The remainder goes to stakers.

$$\text{opTokens} = \lfloor \text{amount} \cdot \text{splitBips} / 10000 \rfloor$$
$$\text{stakerTokens} = \text{amount} - \text{opTokens}$$

| Reward Type | Split Source | Fallback |
|:------------|:-------------|:---------|
| uniqueStake | operatorSetSplit(op, operatorSet) | defaultOperatorSplitBips |
| totalStake | operatorSetSplit(op, operatorSet) | defaultOperatorSplitBips |
| avs | operatorAVSSplit(op, avs) | defaultOperatorSplitBips |
| rewardsForAll | **Always 0** | — |
| rewardsForAllEarners | operatorPISplit(op) | defaultOperatorSplitBips |
| operatorDirectedAVS | operatorAVSSplit(op, avs) | defaultOperatorSplitBips |
| operatorDirectedOperatorSet | operatorSetSplit(op, operatorSet) | defaultOperatorSplitBips |

---

## Integer Arithmetic

Two distribution mechanics are used:

**Direct pro-rata** (operator-level distribution):

$$\text{proRata}_o = \lfloor R \cdot w_o / W \rfloor$$

**15-decimal fixed-point proportion** (staker-level distribution):

$$p_i = \lfloor w_i / T \cdot 10^{15} \rfloor \cdot \frac{1}{10^{15}}$$
$$\text{tokens}_i = \lfloor p_i \cdot \text{pool} \rfloor$$

The two-step fixed-point approach at staker level matches the sidecar's `NUMERIC(38,15)` storage. Operator-level uses direct division because operators are fewer.

| Expression | Dust |
|:-----------|:-----|
| $R = \lfloor \text{amount} / N \rfloor$ | $\text{amount} \bmod N$ lost |
| $\lfloor R \cdot w / W \rfloor$ | Remainder lost |
| $\lfloor w / T \cdot 10^{15} \rfloor / 10^{15}$ | Sub-attotokens |
| $\lfloor p \cdot \text{pool} \rfloor$ | Remainder lost |

Dust accumulates in the RewardsCoordinator contract and is never distributed.

---

## uniqueStake

Operator-set reward weighted by delegated shares, allocation magnitude, and strategy multiplier. Operators with zero allocation receive nothing.

**Eligibility:**

- Operator MUST be a member of `operatorSet`
- Operator MUST have $\alpha_o(s) > 0$ for at least one strategy $s \in S$
- Staker MUST be delegated to an eligible operator
- Staker MUST have $\text{shares}_i(s) > 0$ for at least one $s \in S$

**Split:** operatorSetSplit (fallback: defaultOperatorSplitBips)

**Step 1 — Operator weight:**

$$w_o = \sum_{s \in S} \text{opShares}_o(s) \cdot \alpha_o(s) \cdot m_s$$

**Step 2 — Pro-rata distribution + split:**

$$W = \sum_{o} w_o$$

If $W = 0$: full daily rate $R$ is refunded to AVS. Skip remaining steps.

$$\text{proRata}_o = \lfloor R \cdot w_o / W \rfloor$$
$$\text{opTokens}_o = \lfloor \text{proRata}_o \cdot \text{splitBips} / 10000 \rfloor$$
$$\text{stakerPool}_o = \text{proRata}_o - \text{opTokens}_o$$

**Step 3 — Staker distribution (per operator):**

$$w_i = \sum_{s \in S} \text{shares}_i(s) \cdot \alpha_o(s) \cdot m_s$$

$$T_o = \sum_{i \in \text{stakers}(o)} w_i$$

If $T_o = 0$: stakerPool becomes dust (unclaimable).

$$p_i = \lfloor w_i / T_o \cdot 10^{15} \rfloor \cdot \frac{1}{10^{15}}$$
$$\text{stakerTokens}_i = \lfloor p_i \cdot \text{stakerPool}_o \rfloor$$

A staker whose shares are only in strategies where $\alpha_o(s) = 0$ has weight zero and receives nothing.

---

## totalStake

Operator-set reward weighted by delegated shares and strategy multiplier. Unlike uniqueStake, magnitude is NOT used — raw delegated shares determine weight. An operator with zero allocation but nonzero shares still earns.

**Eligibility:**

- Operator MUST be a member of `operatorSet` (zero allocation OK)
- Staker MUST be delegated to an eligible operator
- Staker MUST have $\text{shares}_i(s) > 0$ for at least one $s \in S$

**Split:** operatorSetSplit (fallback: defaultOperatorSplitBips)

**Step 1 — Operator weight:**

$$w_o = \sum_{s \in S} \text{opShares}_o(s) \cdot m_s$$

**Steps 2–3:** Same as uniqueStake, but staker weight also ignores magnitude:

$$w_i = \sum_{s \in S} \text{shares}_i(s) \cdot m_s$$

---

## avs

Staker-centric reward where all eligible stakers compete in one global pool. Each staker's payout is split between the staker and their operator.

Uses `AVSDirectory` for operator registration.

**Eligibility:**

- Operator MUST be registered to this AVS
- Operator MUST have the strategy restaked with the AVS
- Staker MUST be delegated to an eligible operator
- Staker MUST have $\text{shares}_i(s) > 0$ for at least one $s \in S$ where the operator has that strategy restaked

**Split:** operatorAVSSplit (fallback: defaultOperatorSplitBips)

**Step 1 — Global total weight:**

$$w_i = \sum_{s \in S} \text{shares}_i(s) \cdot m_s$$
$$W = \sum_{i \in \text{eligible}} w_i$$

If $W = 0$: no tokens distributed for this day.

**Step 2 — Per-staker payout + split:**

$$p_i = \lfloor w_i / W \cdot 10^{15} \rfloor \cdot \frac{1}{10^{15}}$$
$$\text{payout}_i = \lfloor p_i \cdot R \rfloor$$
$$\text{opTokens}_i = \lfloor \text{payout}_i \cdot \text{splitBips} / 10000 \rfloor$$
$$\text{stakerTokens}_i = \text{payout}_i - \text{opTokens}_i$$

**Step 3 — Operator total:**

$$\text{operatorTotal}_o = \sum_{i \in \text{stakers}(o)} \text{opTokens}_i$$

---

## rewardsForAll

Staker-centric reward for ALL stakers with shares, regardless of delegation. The operator split is always zero — operators earn nothing.

**Eligibility:**

- Staker MUST have $\text{shares}_i(s) > 0$ for at least one $s \in S$
- Delegation is NOT required

**Split:** None. Always 0. All tokens go to stakers.

**Distribution:**

$$w_i = \sum_{s \in S} \text{shares}_i(s) \cdot m_s$$
$$W = \sum_{i \in \text{eligible}} w_i$$

If $W = 0$: no tokens distributed.

$$p_i = \lfloor w_i / W \cdot 10^{15} \rfloor \cdot \frac{1}{10^{15}}$$
$$\text{stakerTokens}_i = \lfloor p_i \cdot R \rfloor$$

---

## rewardsForAllEarners

Staker-centric reward for stakers delegated to operators that are opted into any AVS or operator set. Uses the operator's protocol-incentive (PI) split.

**Eligibility:**

- Operator MUST be opted into at least one AVS or operator set
- Staker MUST be delegated to an eligible operator
- Staker MUST have $\text{shares}_i(s) > 0$ for at least one $s \in S$
- Staker MUST NOT be on the excluded addresses list (pre-Panama fork only)

**Split:** operatorPISplit (fallback: defaultOperatorSplitBips)

**Distribution:** Same formula as avs type.

---

## operatorDirectedAVS

The AVS specifies a fixed reward amount per operator. Amounts are distributed only on days the operator was registered. Staker distribution within each operator uses the standard per-operator formula.

Uses `AVSDirectory` for operator registration.

**Eligibility:**

- Operator MUST be listed in `operatorRewards[]`
- Operator MUST be registered to this AVS on the snapshot day (checked per day)
- Staker MUST be delegated to an eligible operator
- Staker MUST have $\text{shares}_i(s) > 0$ for at least one $s \in S$

**Split:** operatorAVSSplit (fallback: defaultOperatorSplitBips)

**Step 1 — Registration count:**

For each operator $o$ in `operatorRewards`:

$$n_o = \text{number of snapshot days where } o \text{ is registered to this AVS}$$

**Step 2a — If $n_o = 0$ (never registered during window):**

$$\text{refund per day} = \lfloor \text{amount}_o / N \rfloor \quad \rightarrow \text{AVS}$$

**Step 2b — If $n_o > 0$ AND registered on this snapshot day:**

$$R_o = \lfloor \text{amount}_o / n_o \rfloor$$
$$\text{opTokens}_o = \lfloor R_o \cdot \text{splitBips} / 10000 \rfloor$$
$$\text{stakerPool}_o = R_o - \text{opTokens}_o$$

**Step 2c — If $n_o > 0$ but NOT registered on this day:** No distribution. The per-registered-day amount is already larger to compensate.

**Step 3 — Staker distribution (per operator):**

$$w_i = \sum_{s \in S} \text{shares}_i(s) \cdot m_s$$
$$T_o = \sum_{i \in \text{stakers}(o)} w_i$$

If $T_o = 0$: stakerPool becomes dust.

$$p_i = \lfloor w_i / T_o \cdot 10^{15} \rfloor \cdot \frac{1}{10^{15}}$$
$$\text{stakerTokens}_i = \lfloor p_i \cdot \text{stakerPool}_o \rfloor$$

---

## operatorDirectedOperatorSet

The AVS specifies a fixed reward amount per operator, scoped to an operator set. Amounts are distributed only on days the operator was a member. Staker distribution within each operator uses the standard per-operator formula.

**Eligibility:**

- Operator MUST be listed in `operatorRewards[]`
- Operator MUST be a member of `operatorSet` on the snapshot day (checked per day)
- Each strategy $s \in S$ MUST be registered in `operatorSet`. Unregistered strategies are excluded from staker weight.
- Staker MUST be delegated to an eligible operator
- Staker MUST have $\text{shares}_i(s) > 0$ for at least one registered $s \in S$

**Split:** operatorSetSplit (fallback: defaultOperatorSplitBips)

**Step 1 — Membership count:**

$$n_o = \text{number of snapshot days where } o \text{ is a member of operatorSet}$$

**Step 2a — If $n_o = 0$:** Refund $\lfloor \text{amount}_o / N \rfloor$ per day to AVS.

**Step 2b — If $n_o > 0$ AND a member on this day:**

$$R_o = \lfloor \text{amount}_o / n_o \rfloor$$
$$\text{opTokens}_o = \lfloor R_o \cdot \text{splitBips} / 10000 \rfloor$$
$$\text{stakerPool}_o = R_o - \text{opTokens}_o$$

**Step 2b′ — Strategy-registration refund:** If the operator IS a member but NO strategies in $S$ are registered in the operator set on this day, the operator still receives opTokens but stakerPool is refunded to AVS.

**Step 2c — Not a member on this day:** No distribution.

**Step 3 — Staker distribution:**

$$w_i = \sum_{\substack{s \in S \\ s \text{ registered in opSet}}} \text{shares}_i(s) \cdot m_s$$

$$T_o = \sum_{i \in \text{stakers}(o)} w_i$$

If $T_o = 0$: stakerPool becomes dust.

$$p_i = \lfloor w_i / T_o \cdot 10^{15} \rfloor \cdot \frac{1}{10^{15}}$$
$$\text{stakerTokens}_i = \lfloor p_i \cdot \text{stakerPool}_o \rfloor$$

---

## Submission Constraints

**All types:**

- `startTimestamp` MUST be ≡ 0 (mod 86400) (midnight-aligned)
- `duration` MUST be ≡ 0 (mod 86400), > 0, ≤ MAX_REWARDS_DURATION
- `startTimestamp` MUST be ≥ GENESIS_REWARDS_TIMESTAMP
- `startTimestamp` MUST be ≥ now − MAX_RETROACTIVE_LENGTH
- `strategiesAndMultipliers[]` MUST be sorted ascending by strategy address

**Standard rewards** (uniqueStake, totalStake, avs, rewardsForAll, rewardsForAllEarners):

- `startTimestamp` MUST be ≤ now + MAX_FUTURE_LENGTH

**Operator-directed** (operatorDirectedAVS, operatorDirectedOperatorSet):

- Entire reward window MUST be in past: $t_s + D < \text{now}$
- `operatorRewards[]` MUST be sorted ascending by operator address
- Each operator address MUST NOT be zero address
- Each operator amount MUST be > 0

---

## Edge Cases

**Zero total weight ($W = 0$):**

- uniqueStake/totalStake: Full daily rate refunded to AVS
- Other types: Undistributed (remains in RewardsCoordinator contract)

**Zero staker weight ($T_o = 0$):**

- Staker pool becomes dust (unclaimable)

**Dust accumulation:**

- $\text{amount} \bmod N$ lost in daily rate calculation
- Remainders from floor division lost at each step
- Accumulates in RewardsCoordinator contract, never distributed

**Slashing during reward period:**

- Shares reduced immediately at slash block
- Affects all subsequent snapshot days
- Queued withdrawals still slashable during delay
