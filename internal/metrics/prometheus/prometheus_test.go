package prometheus

import (
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/metrics/metricsTypes"
	"github.com/stretchr/testify/assert"
	"testing"
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
	t.Run("Should return an error for no labels", func(t *testing.T) {
		err := pmc.hasUnexpectedLabels(metricsTypes.MetricsType_Timing, metricsTypes.Metric_Timing_BlockProcessDuration, []metricsTypes.MetricsLabel{})
		assert.NotNil(t, err)
	})
	t.Run("Should return an error for unexpected labels", func(t *testing.T) {
		err := pmc.hasUnexpectedLabels(metricsTypes.MetricsType_Timing, metricsTypes.Metric_Timing_BlockProcessDuration, []metricsTypes.MetricsLabel{
			{Name: "rewardsCalculated", Value: "10"},
			{Name: "hasError", Value: "false"},
			{Name: "unexpectedLabel", Value: "unexpectedValue"},
		})
		assert.NotNil(t, err)
	})

}
