import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

const sidebar: SidebarsConfig = {
  apisidebar: [
    {
      type: "doc",
      id: "api/eigenlayer-api-specification",
    },
    {
      type: "category",
      label: "eigenlayer/pds/aprs/v1",
      items: [
        {
          type: "doc",
          id: "api/aprs-get-daily-apr-for-earner-strategy",
          label: "GetDailyAprForEarnerStrategy",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/aprs-get-daily-operator-strategy-aprs",
          label: "GetDailyOperatorStrategyAprs",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "events",
      items: [
        {
          type: "doc",
          id: "api/events-stream-eigen-state-changes",
          label: "StreamEigenStateChanges",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api/events-stream-indexed-blocks",
          label: "StreamIndexedBlocks",
          className: "api-method post",
        },
      ],
    },
    {
      type: "category",
      label: "protocol",
      items: [
        {
          type: "doc",
          id: "api/protocol-get-eigen-state-changes",
          label: "GetEigenStateChanges",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/protocol-list-operator-queued-withdrawals",
          label: "ListOperatorQueuedWithdrawals",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/protocol-list-operator-strategy-queued-withdrawals",
          label: "ListOperatorStrategyQueuedWithdrawals",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/protocol-get-delegated-stakers-for-operator",
          label: "GetDelegatedStakersForOperator",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/protocol-get-delegated-strategies-for-operator",
          label: "GetDelegatedStrategiesForOperator",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/protocol-get-registered-avs-for-operator",
          label: "GetRegisteredAvsForOperator",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/protocol-get-operator-delegated-stake-for-strategy",
          label: "GetOperatorDelegatedStakeForStrategy",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/protocol-list-staker-queued-withdrawals",
          label: "ListStakerQueuedWithdrawals",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/protocol-list-staker-strategies",
          label: "ListStakerStrategies",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/protocol-get-strategy-for-staker",
          label: "GetStrategyForStaker",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/protocol-get-staker-shares",
          label: "GetStakerShares",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/protocol-list-strategies",
          label: "ListStrategies",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/protocol-list-strategy-queued-withdrawals",
          label: "ListStrategyQueuedWithdrawals",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "rewards",
      items: [
        {
          type: "doc",
          id: "api/rewards-get-attributable-rewards-for-distribution-root",
          label: "GetAttributableRewardsForDistributionRoot",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/rewards-get-attributable-rewards-for-snapshot",
          label: "GetAttributableRewardsForSnapshot",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/rewards-get-rewards-by-avs-for-distribution-root",
          label: "GetRewardsByAvsForDistributionRoot",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/rewards-backfill-staker-operators",
          label: "BackfillStakerOperators",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api/rewards-get-claimed-rewards-by-block",
          label: "GetClaimedRewardsByBlock",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/rewards-get-rewards-root",
          label: "GetRewardsRoot",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/rewards-generate-claim-proof",
          label: "GenerateClaimProof",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api/rewards-list-distribution-roots",
          label: "ListDistributionRoots",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/rewards-list-earner-historical-rewards",
          label: "ListEarnerHistoricalRewards",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/rewards-list-earner-lifetime-rewards",
          label: "ListEarnerLifetimeRewards",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/rewards-get-available-rewards-tokens",
          label: "GetAvailableRewardsTokens",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/rewards-get-claimable-rewards",
          label: "GetClaimableRewards",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/rewards-list-claimed-rewards-by-block-range",
          label: "ListClaimedRewardsByBlockRange",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/rewards-get-summarized-rewards-for-earner",
          label: "GetSummarizedRewardsForEarner",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/rewards-get-total-claimed-rewards",
          label: "GetTotalClaimedRewards",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/rewards-generate-rewards",
          label: "GenerateRewards",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api/rewards-generate-rewards-root",
          label: "GenerateRewardsRoot",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api/rewards-generate-staker-operators",
          label: "GenerateStakerOperators",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api/rewards-get-rewards-for-snapshot",
          label: "GetRewardsForSnapshot",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "sidecar",
      items: [
        {
          type: "doc",
          id: "api/rpc-about",
          label: "About",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/rpc-get-block-height",
          label: "GetBlockHeight",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/rpc-get-state-root",
          label: "GetStateRoot",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "health",
      items: [
        {
          type: "doc",
          id: "api/health-health-check",
          label: "HealthCheck",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/health-ready-check",
          label: "ReadyCheck",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "operatorSets",
      items: [
        {
          type: "doc",
          id: "api/operator-sets-list-operators-for-avs",
          label: "ListOperatorsForAvs",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/operator-sets-list-operators-for-staker",
          label: "ListOperatorsForStaker",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/operator-sets-list-operators-for-strategy",
          label: "ListOperatorsForStrategy",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "slashing",
      items: [
        {
          type: "doc",
          id: "api/slashing-list-avs-operator-set-slashing-history",
          label: "ListAvsOperatorSetSlashingHistory",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/slashing-list-avs-slashing-history",
          label: "ListAvsSlashingHistory",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/slashing-list-operator-slashing-history",
          label: "ListOperatorSlashingHistory",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api/slashing-list-staker-slashing-history",
          label: "ListStakerSlashingHistory",
          className: "api-method get",
        },
      ],
    },
  ],
};

export default sidebar.apisidebar;
