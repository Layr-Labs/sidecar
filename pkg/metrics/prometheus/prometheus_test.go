package prometheus

import (
	"testing"

	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/metrics/metricsTypes"
	"github.com/stretchr/testify/assert"
)

func Test_UnexpectedLabelsParsing(t *testing.T) {
	l, err := logger.NewLogger(&logger.LoggerConfig{Debug: false})
	assert.Nil(t, err)

	pmc, err := NewPrometheusMetricsClient(&PrometheusMetricsConfig{
		Metrics: metricsTypes.MetricTypes,
	}, l)
	assert.Nil(t, err)

	t.Run("Should return no error for all labels", func(t *testing.T) {
		err := pmc.hasUnexpectedLabels(metricsTypes.MetricsType_Timing, metricsTypes.Metric_Timing_BlockProcessDuration, []metricsTypes.MetricsLabel{
			{Name: "rewardsCalculated", Value: "10"},
			{Name: "hasError", Value: "false"},
		})
		assert.Nil(t, err)
	})
	t.Run("Should return no error for a subset labels", func(t *testing.T) {
		err := pmc.hasUnexpectedLabels(metricsTypes.MetricsType_Timing, metricsTypes.Metric_Timing_BlockProcessDuration, []metricsTypes.MetricsLabel{
			{Name: "rewardsCalculated", Value: "10"},
		})
		assert.Nil(t, err)
	})
	t.Run("Should return an error for unexpected labels", func(t *testing.T) {
		err := pmc.hasUnexpectedLabels(metricsTypes.MetricsType_Timing, metricsTypes.Metric_Timing_BlockProcessDuration, []metricsTypes.MetricsLabel{
			{Name: "rewardsCalculated", Value: "10"},
			{Name: "hasError", Value: "false"},
			{Name: "unexpectedLabel", Value: "unexpectedValue"},
		})
		assert.NotNil(t, err)
	})
	t.Run("Should return an error for unexpected labels when expecting 0 labels", func(t *testing.T) {
		err := pmc.hasUnexpectedLabels(metricsTypes.MetricsType_Gauge, metricsTypes.Metric_Gauge_CurrentBlockHeight, []metricsTypes.MetricsLabel{
			{Name: "rewardsCalculated", Value: "10"},
			{Name: "hasError", Value: "false"},
			{Name: "unexpectedLabel", Value: "unexpectedValue"},
		})
		assert.NotNil(t, err)
	})

}
