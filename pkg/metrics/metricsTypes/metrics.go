package metricsTypes

import "time"

type IMetricsClient interface {
	Incr(name string, labels []MetricsLabel, value float64) error
	Gauge(name string, value float64, labels []MetricsLabel) error
	Timing(name string, value time.Duration, labels []MetricsLabel) error
	Flush()
}

type MetricsLabel struct {
	Name  string
	Value string
}

type MetricsType string

var (
	MetricsType_Incr   MetricsType = "incr"
	MetricsType_Gauge  MetricsType = "gauge"
	MetricsType_Timing MetricsType = "timing"
)

type MetricsTypeConfig struct {
	Name   string
	Labels []string
}

var (
	Metric_Incr_BlockProcessed             = "blockProcessed"
	Metric_Incr_GrpcRequest                = "rpc.grpc.request"
	Metric_Incr_HttpRequest                = "rpc.http.request"
	Metric_Incr_RewardsRootVerified        = "indexer.rewardsRootVerified"
	Metric_Incr_RewardsRootInvalid         = "indexer.rewardsRootInvalid"
	Metric_Incr_RewardsMerkelizationFailed = "indexer.rewardsWerkelizationFailed"
	Metric_Incr_ProofDataCacheHit          = "proofData.cache.hit"
	Metric_Incr_ProofDataCacheMiss         = "proofData.cache.miss"
	Metric_Incr_ProofDataCacheEvicted      = "proofData.cache.evicted"

	Metric_Gauge_CurrentBlockHeight = "currentBlockHeight"
	Metric_Gauge_SnapshotSize       = "snapshots.create.size"

	Metric_Gauge_LastDistributionRootBlockHeight = "lastDistributionRootBlockHeight"

	Metric_Timing_GrpcDuration         = "rpc.grpc.duration"
	Metric_Timing_HttpDuration         = "rpc.http.duration"
	Metric_Timing_RewardsCalcDuration  = "rewards.duration"
	Metric_Timing_BlockProcessDuration = "block.process.duration"
	Metric_Timing_CreateSnapshot       = "snapshots.create.duration"
)

var MetricTypes = map[MetricsType][]MetricsTypeConfig{
	MetricsType_Incr: {
		MetricsTypeConfig{
			Name:   Metric_Incr_BlockProcessed,
			Labels: []string{},
		},
		MetricsTypeConfig{
			Name: Metric_Incr_GrpcRequest,
			Labels: []string{
				"grpc_method",
				"status",
				"status_code",
				"rpc",
				"client_source",
				"client_type",
			},
		},
		MetricsTypeConfig{
			Name: Metric_Incr_HttpRequest,
			Labels: []string{
				"method",
				"path",
				"status_code",
				"grpc_method",
				"pattern",
				"rpc",
				"client_ip",
				"client_source",
				"client_type",
			},
		},
		MetricsTypeConfig{
			Name: Metric_Incr_RewardsRootVerified,
			Labels: []string{
				"block_number",
			},
		},
		MetricsTypeConfig{
			Name: Metric_Incr_RewardsRootInvalid,
			Labels: []string{
				"block_number",
			},
		},
		MetricsTypeConfig{
			Name: Metric_Incr_RewardsMerkelizationFailed,
			Labels: []string{
				"block_number",
			},
		},
		MetricsTypeConfig{
			Name: Metric_Incr_ProofDataCacheHit,
			Labels: []string{
				"snapshot",
			},
		},
		MetricsTypeConfig{
			Name: Metric_Incr_ProofDataCacheMiss,
			Labels: []string{
				"snapshot",
			},
		},
		MetricsTypeConfig{
			Name: Metric_Incr_ProofDataCacheEvicted,
			Labels: []string{
				"snapshot",
			},
		},
	},
	MetricsType_Gauge: {
		MetricsTypeConfig{
			Name:   Metric_Gauge_CurrentBlockHeight,
			Labels: []string{},
		},
		MetricsTypeConfig{
			Name:   Metric_Gauge_SnapshotSize,
			Labels: []string{},
		},
	},
	MetricsType_Timing: {
		MetricsTypeConfig{
			Name: Metric_Timing_GrpcDuration,
			Labels: []string{
				"grpc_method",
				"status",
				"status_code",
				"rpc",
				"client_source",
				"client_type",
			},
		},
		MetricsTypeConfig{
			Name: Metric_Timing_HttpDuration,
			Labels: []string{
				"method",
				"path",
				"status_code",
				"grpc_method",
				"pattern",
				"rpc",
				"client_ip",
				"client_source",
				"client_type",
			},
		},
		MetricsTypeConfig{
			Name: Metric_Timing_RewardsCalcDuration,
			Labels: []string{
				"snapshotDate",
			},
		},
		MetricsTypeConfig{
			Name: Metric_Timing_BlockProcessDuration,
			Labels: []string{
				"rewardsCalculated",
				"hasError",
			},
		},
		MetricsTypeConfig{
			Name: Metric_Timing_CreateSnapshot,
			Labels: []string{
				"chain",
				"sidecarVersion",
				"kind",
			},
		},
	},
}
