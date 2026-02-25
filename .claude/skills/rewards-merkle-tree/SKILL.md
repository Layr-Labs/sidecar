---
name: rewards-merkle-tree
description: EigenLayer RewardsCoordinator merkle tree structure and claim verification. Use when working with rewards distribution roots, claim proofs, or cumulative earnings calculations.
---

# RewardsCoordinator Merkle Tree Reference

## Tree Structure

Two-level merkle tree for rewards distribution:

```
Distribution Root (bytes32)
├── EarnerLeaf: hash(EARNER_LEAF_SALT, earner, earnerTokenRoot)
│   ├── TokenLeaf: hash(TOKEN_LEAF_SALT, token, cumulativeEarnings)
│   └── TokenLeaf: hash(TOKEN_LEAF_SALT, token, cumulativeEarnings)
└── EarnerLeaf: ...
```

## On-Chain Data Structures

```solidity
struct EarnerTreeMerkleLeaf {
    address earner;
    bytes32 earnerTokenRoot;  // Root of this earner's token subtree
}

struct TokenTreeMerkleLeaf {
    IERC20 token;
    uint256 cumulativeEarnings;  // Lifetime total, not per-period
}

struct RewardsMerkleClaim {
    uint32 rootIndex;
    uint32 earnerIndex;
    bytes earnerTreeProof;
    EarnerTreeMerkleLeaf earnerLeaf;
    uint32[] tokenIndices;
    bytes[] tokenTreeProofs;
    TokenTreeMerkleLeaf[] tokenLeaves;
}
```

## Hash Constants

```solidity
bytes32 constant EARNER_LEAF_SALT = keccak256("EARNER_LEAF_SALT");
// = 0x0eb...  (compute once, cache)

bytes32 constant TOKEN_LEAF_SALT = keccak256("TOKEN_LEAF_SALT");
// = 0x1de...  (compute once, cache)
```

## Leaf Hash Formulas

**Earner leaf:**
```solidity
keccak256(abi.encodePacked(EARNER_LEAF_SALT, earner, earnerTokenRoot))
// = keccak256(32 bytes salt || 20 bytes address || 32 bytes root)
```

**Token leaf:**
```solidity
keccak256(abi.encodePacked(TOKEN_LEAF_SALT, token, cumulativeEarnings))
// = keccak256(32 bytes salt || 20 bytes address || 32 bytes uint256)
```

## Claim Verification (On-Chain)

```solidity
function processClaim(RewardsMerkleClaim calldata claim, address recipient) external {
    DistributionRoot memory root = _distributionRoots[claim.rootIndex];
    require(block.timestamp >= root.activatedAt, "root not active");
    require(!root.disabled, "root disabled");
    
    // 1. Verify earner leaf in distribution root
    _verifyEarnerClaimProof(root.root, claim);
    
    // 2. For each token, verify in earner's token tree
    for (uint i = 0; i < claim.tokenLeaves.length; i++) {
        _verifyTokenClaimProof(claim.earnerLeaf.earnerTokenRoot, claim, i);
        
        // 3. Calculate claimable = cumulative - alreadyClaimed
        uint256 claimable = claim.tokenLeaves[i].cumulativeEarnings 
                          - cumulativeClaimed[claim.earnerLeaf.earner][claim.tokenLeaves[i].token];
        
        // 4. Update claimed and transfer
        cumulativeClaimed[claim.earnerLeaf.earner][claim.tokenLeaves[i].token] = 
            claim.tokenLeaves[i].cumulativeEarnings;
        claim.tokenLeaves[i].token.safeTransfer(recipient, claimable);
    }
}
```

## Merkle Proof Verification

Uses OpenZeppelin's `Merkle.verifyInclusionKeccak`:

```solidity
function _verifyEarnerClaimProof(bytes32 root, RewardsMerkleClaim calldata claim) internal pure {
    bytes32 earnerLeafHash = keccak256(
        abi.encodePacked(EARNER_LEAF_SALT, claim.earnerLeaf.earner, claim.earnerLeaf.earnerTokenRoot)
    );
    require(
        Merkle.verifyInclusionKeccak(
            claim.earnerTreeProof,
            root,
            earnerLeafHash,
            claim.earnerIndex
        ),
        "invalid earner proof"
    );
}
```

## Root Submission Constraints

```solidity
function submitRoot(bytes32 root, uint32 rewardsCalculationEndTimestamp) external onlyRewardsUpdater {
    require(rewardsCalculationEndTimestamp > currRewardsCalculationEndTimestamp, "must be newer");
    require(rewardsCalculationEndTimestamp < block.timestamp, "must be in past");
    
    uint32 rootIndex = uint32(_distributionRoots.length);
    uint32 activatedAt = uint32(block.timestamp) + activationDelay;
    
    _distributionRoots.push(DistributionRoot({
        root: root,
        activatedAt: activatedAt,
        rewardsCalculationEndTimestamp: rewardsCalculationEndTimestamp,
        disabled: false
    }));
    
    currRewardsCalculationEndTimestamp = rewardsCalculationEndTimestamp;
    emit DistributionRootSubmitted(rootIndex, root, rewardsCalculationEndTimestamp, activatedAt);
}
```

## Key Behaviors

**Cumulative earnings:**
- Token leaves contain LIFETIME cumulative earnings, not per-period
- Claimable amount = `cumulativeEarnings - cumulativeClaimed[earner][token]`
- Must track and add to previous cumulative, never replace

**Ordering requirements:**
- Earner leaves: sorted by earner address (ascending)
- Token leaves: sorted by token address (ascending)
- Required for deterministic tree construction

**Activation delay:**
- Roots not claimable until `block.timestamp >= activatedAt`
- `activatedAt = submissionTime + activationDelay`
- Allows time to disable invalid roots

**Root disabling:**
- Only `rewardsUpdater` can disable
- Only before `activatedAt`
- Prevents claims from invalid roots

## Reward Submission Validation

```solidity
// Duration constraints
require(duration > 0 && duration <= MAX_REWARDS_DURATION);  // MAX = 604800 (1 week)
require(duration % CALCULATION_INTERVAL_SECONDS == 0);       // INTERVAL = 3600 (1 hour)

// Timestamp constraints  
require(startTimestamp % CALCULATION_INTERVAL_SECONDS == 0);
require(startTimestamp >= GENESIS_REWARDS_TIMESTAMP);
require(startTimestamp >= block.timestamp - MAX_RETROACTIVE_LENGTH);  // 6 months
require(startTimestamp <= block.timestamp + MAX_FUTURE_LENGTH);       // 1 day

// Amount constraints
require(amount > 0 && amount <= MAX_REWARDS_AMOUNT);

// Strategy constraints
// Must be sorted ascending, no duplicates, all whitelisted
```

## Protocol Fee (Operator-Directed Only)

```solidity
if (isOptedInForProtocolFee[msg.sender]) {
    uint256 fee = amount * PROTOCOL_FEE_BIPS / 10000;  // Default 10%
    token.safeTransferFrom(msg.sender, feeRecipient, fee);
    amount -= fee;
}
```
