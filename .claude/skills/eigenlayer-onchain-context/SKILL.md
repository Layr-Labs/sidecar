---
name: eigenlayer-onchain-context
description: EigenLayer smart contract behaviors and events. Use when working with code that indexes EigenLayer events, tracks shares/delegations/slashing, or calculates rewards distributions.
---

# EigenLayer On-Chain Contract Reference

## DelegationManager

Handles staker-to-operator delegation and withdrawal queuing.

**Key behaviors:**
- Staker delegates to exactly ONE operator at a time
- Undelegation automatically queues ALL staker shares for withdrawal
- Deposits while delegated auto-delegate to current operator

**Events:**

| Event | Emitted When | Key Fields |
|-------|--------------|------------|
| `StakerDelegated` | `delegateTo()` or `redelegate()` | staker, operator |
| `StakerUndelegated` | `undelegate()` or `redelegate()` | staker, operator |
| `OperatorSharesIncreased` | Deposit while delegated, delegation with shares, withdrawal re-deposit | operator, staker, strategy, shares (DELTA) |
| `OperatorSharesDecreased` | Withdrawal queued, undelegation, slashing | operator, staker, strategy, shares (ABS value) |
| `SlashingWithdrawalQueued` | `queueWithdrawals()` | withdrawalRoot, Withdrawal{staker, delegatedTo, startBlock, strategies[], scaledShares[]} |
| `SlashingWithdrawalCompleted` | `completeQueuedWithdrawal()` | withdrawalRoot |
| `OperatorSharesSlashed` | AllocationManager slashing | operator, strategy, totalSlashedShares |
| `WithdrawalQueued` | Legacy M2 withdrawal | (legacy) |
| `WithdrawalMigrated` | Legacy migration - cancels corresponding WithdrawalQueued | (legacy) |

## StrategyManager

Entry point for LST/ERC20 deposits.

**Events:**

| Event | Emitted When | Key Fields |
|-------|--------------|------------|
| `Deposit` | `depositIntoStrategy()` | staker, strategy, shares (received, not token amount) |
| `ShareWithdrawalQueued` | Legacy M1 withdrawal | (legacy) |

## EigenPodManager

Native ETH restaking via EigenPods.

**Key behaviors:**
- `sharesDelta` is SIGNED (positive=deposit/rewards, negative=slash/withdrawal)
- Strategy is always `beaconChainETHStrategy` (virtual)
- Shares in wei (1e18 = 1 ETH)

**Events:**

| Event | Emitted When | Key Fields |
|-------|--------------|------------|
| `PodSharesUpdated` | Beacon chain balance update | podOwner, sharesDelta (int256) |
| `BeaconChainSlashingFactorDecreased` | Beacon chain slashing detected | staker, prevFactor, newFactor (WAD, monotonic decrease) |

## AllocationManager

Operator allocations to operator sets and slashing.

**Key behaviors:**
- Magnitude in WAD (1e18 = 100%)
- Allocations have delay before becoming slashable (`effectBlock`)
- Deallocations have DEALLOCATION_DELAY before magnitude freed
- `allocatableMagnitude = maxMagnitude - encumberedMagnitude`
- Operator remains slashable for DEALLOCATION_DELAY after removal from set

**Events:**

| Event | Emitted When | Key Fields |
|-------|--------------|------------|
| `OperatorSlashed` | AVS calls `slashOperator()` | operator, operatorSet, strategies[], wadSlashed[] (proportion in WAD) |
| `AllocationUpdated` | `modifyAllocations()` | operator, operatorSet, strategy, magnitude, effectBlock |
| `MaxMagnitudeUpdated` | After slashing | operator, strategy, maxMagnitude (starts 1e18, only decreases) |
| `EncumberedMagnitudeUpdated` | Allocation changes | operator, strategy, encumberedMagnitude |
| `OperatorSetCreated` | AVS creates set | operatorSet{avs, id} |
| `OperatorAddedToOperatorSet` | Operator registers | operator, operatorSet |
| `OperatorRemovedFromOperatorSet` | Deregistration | operator, operatorSet |
| `StrategyAddedToOperatorSet` | AVS adds strategy | operatorSet, strategy |
| `StrategyRemovedFromOperatorSet` | AVS removes strategy | operatorSet, strategy |
| `AllocationDelaySet` | Operator sets delay | operator, delay, effectBlock |

## AVSDirectory (Legacy)

Legacy AVS-operator registration (pre-operator sets).

**Events:**

| Event | Emitted When | Key Fields |
|-------|--------------|------------|
| `OperatorAVSRegistrationStatusUpdated` | Legacy registration | operator, avs, status (0=UNREGISTERED, 1=REGISTERED) |

## RewardsCoordinator

Reward submissions, splits, and claims.

**Reward Submission Events:**

| Event | Type | Key Fields |
|-------|------|------------|
| `AVSRewardsSubmissionCreated` | Proportional to stake | avs, submissionNonce, RewardsSubmission{strategiesAndMultipliers[], token, amount, startTimestamp, duration} |
| `OperatorDirectedAVSRewardsSubmissionCreated` | Direct per-operator amounts | Contains operatorRewards[] with specific amounts |
| `OperatorDirectedOperatorSetRewardsSubmissionCreated` | Direct, scoped to operator set | operatorSet, operatorRewards[] |
| `RewardsSubmissionForAllCreated` | Protocol-wide, all stakers | Programmatic incentives |
| `RewardsSubmissionForAllEarnersCreated` | Protocol-wide, all earners | Programmatic incentives |

**Split Events (all have activation delay!):**

| Event | Scope | Key Fields |
|-------|-------|------------|
| `DefaultOperatorSplitBipsSet` | Protocol default | oldSplit, newSplit (basis points, 10000=100%) |
| `OperatorAVSSplitBipsSet` | Operator-AVS pair | operator, avs, activatedAt, oldSplit, newSplit |
| `OperatorPISplitBipsSet` | Programmatic Incentives | operator, activatedAt, oldSplit, newSplit |
| `OperatorSetSplitBipsSet` | Operator set | operator, operatorSet, activatedAt, oldSplit, newSplit |

**Distribution Events:**

| Event | Emitted When | Key Fields |
|-------|--------------|------------|
| `DistributionRootSubmitted` | Root submitted | rootIndex, root, rewardsCalculationEndTimestamp, activatedAt (claimable after) |
| `DistributionRootDisabled` | Root cancelled before activation | rootIndex |

---

## Critical On-Chain Behaviors

### Slashing

**AVS Slashing** (via AllocationManager):
- Reduces operator's `maxMagnitude` and `currentMagnitude`
- Triggers `OperatorSharesSlashed` in DelegationManager
- Affects all delegated stakers proportionally

**Beacon Chain Slashing** (via EigenPodManager):
- Reduces staker's `beaconChainSlashingFactor`
- Native ETH only, independent of AVS slashing

### Split Priority (for reward distribution)

```
1. OperatorSetSplit (if operator set reward)
2. OperatorAVSSplit (if AVS reward)  
3. OperatorPISplit (if programmatic incentive)
4. DefaultOperatorSplit (fallback)

If block.timestamp < activatedAt: use oldSplit, else newSplit
```

### Withdrawal Delay

- `MIN_WITHDRAWAL_DELAY_BLOCKS` ≈ 7 days
- Queued withdrawals remain slashable during delay
- Slashing during delay reduces final withdrawal amount

### Timing

- Splits and allocations have activation delays
- Retroactive rewards allowed within MAX_RETROACTIVE_LENGTH
- Rewards aligned to CALCULATION_INTERVAL_SECONDS (1 hour)
