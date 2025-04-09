package metrics

import (
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/metrics/dogstatsd"
	"github.com/Layr-Labs/sidecar/pkg/metrics/metricsTypes"
	"github.com/Layr-Labs/sidecar/pkg/metrics/prometheus"
	"go.uber.org/zap"
)

type MetricsSink struct {
	clients []metricsTypes.IMetricsClient
	config  *MetricsSinkConfig
}

type MetricsSinkConfig struct {
	DefaultLabels []metricsTypes.MetricsLabel
}

func NewMetricsSink(cfg *MetricsSinkConfig, clients []metricsTypes.IMetricsClient) (*MetricsSink, error) {
	if cfg.DefaultLabels == nil {
		cfg.DefaultLabels = []metricsTypes.MetricsLabel{}
	}
	return &MetricsSink{
		clients: clients,
		config:  cfg,
	}, nil
}

func mergeLabels(labels []metricsTypes.MetricsLabel, defaultLabels []metricsTypes.MetricsLabel) []metricsTypes.MetricsLabel {
	if labels == nil {
		return defaultLabels
	}
	mergedLabels := make([]metricsTypes.MetricsLabel, 0)
	mergedLabels = append(mergedLabels, defaultLabels...)
	mergedLabels = append(mergedLabels, labels...)
	return mergedLabels
}

func (ms *MetricsSink) Incr(name string, labels []metricsTypes.MetricsLabel, value float64) error {
	mergedLabels := mergeLabels(labels, ms.config.DefaultLabels)
	for _, client := range ms.clients {
		err := client.Incr(name, mergedLabels, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ms *MetricsSink) Gauge(name string, value float64, labels []metricsTypes.MetricsLabel) error {
	mergedLabels := mergeLabels(labels, ms.config.DefaultLabels)
	for _, client := range ms.clients {
		err := client.Gauge(name, value, mergedLabels)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ms *MetricsSink) Timing(name string, value time.Duration, labels []metricsTypes.MetricsLabel) error {
	mergedLabels := mergeLabels(labels, ms.config.DefaultLabels)
	for _, client := range ms.clients {
		err := client.Timing(name, value, mergedLabels)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ms *MetricsSink) Flush() {
	for _, client := range ms.clients {
		client.Flush()
	}
}

func InitMetricsSinksFromConfig(cfg *config.Config, l *zap.Logger) ([]metricsTypes.IMetricsClient, error) {
	clients := []metricsTypes.IMetricsClient{}

	if cfg.DataDogConfig.StatsdConfig.Enabled {
		dd, err := dogstatsd.NewDogStatsdMetricsClient(cfg.DataDogConfig.StatsdConfig.Url, cfg.DataDogConfig.StatsdConfig.SampleRate, l)
		if err != nil {
			return nil, err
		}
		clients = append(clients, dd)
	}

	if cfg.PrometheusConfig.Enabled {
		pm, err := prometheus.NewPrometheusMetricsClient(&prometheus.PrometheusMetricsConfig{
			Metrics: metricsTypes.MetricTypes,
		}, l)
		if err != nil {
			return nil, err
		}
		clients = append(clients, pm)
	}

	return clients, nil
}
