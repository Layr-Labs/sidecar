package prometheus

import (
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/metrics/metricsTypes"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"slices"
	"strings"
	"time"
)

type PrometheusMetricsConfig struct {
	Metrics map[metricsTypes.MetricsType][]metricsTypes.MetricsTypeConfig
}

type PrometheusMetricsClient struct {
	logger *zap.Logger
	config *PrometheusMetricsConfig

	counters   map[string]*prometheus.CounterVec
	gauges     map[string]*prometheus.GaugeVec
	histograms map[string]*prometheus.HistogramVec
}

func NewPrometheusMetricsClient(config *PrometheusMetricsConfig, l *zap.Logger) (*PrometheusMetricsClient, error) {
	client := &PrometheusMetricsClient{
		config: config,
		logger: l,

		counters:   make(map[string]*prometheus.CounterVec),
		gauges:     make(map[string]*prometheus.GaugeVec),
		histograms: make(map[string]*prometheus.HistogramVec),
	}

	client.initializeTypes()

	return client, nil
}

func (pmc *PrometheusMetricsClient) logExistingMetric(t metricsTypes.MetricsType, metric metricsTypes.MetricsTypeConfig) {
	pmc.logger.Sugar().Warnw("Prometheus metric already exists for type",
		zap.String("type", string(t)),
		zap.String("name", metric.Name),
	)
}

func (pmc *PrometheusMetricsClient) initializeTypes() {
	for t, types := range pmc.config.Metrics {
		for _, mt := range types {
			switch t {
			case metricsTypes.MetricsType_Incr:
				if _, ok := pmc.counters[mt.Name]; ok {
					pmc.logExistingMetric(t, mt)
					continue
				}
				pmc.counters[mt.Name] = prometheus.NewCounterVec(prometheus.CounterOpts{
					Name: mt.Name,
				}, mt.Labels)
				prometheus.MustRegister(pmc.counters[mt.Name])
			case metricsTypes.MetricsType_Gauge:
				if _, ok := pmc.counters[mt.Name]; ok {
					pmc.logExistingMetric(t, mt)
					continue
				}
				pmc.gauges[mt.Name] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
					Name: mt.Name,
				}, mt.Labels)
				prometheus.MustRegister(pmc.gauges[mt.Name])
			case metricsTypes.MetricsType_Timing:
				if _, ok := pmc.counters[mt.Name]; ok {
					pmc.logExistingMetric(t, mt)
					continue
				}
				pmc.histograms[mt.Name] = prometheus.NewHistogramVec(prometheus.HistogramOpts{
					Name: mt.Name,
				}, mt.Labels)
				prometheus.MustRegister(pmc.histograms[mt.Name])
			}
		}
	}
}

func (pmc *PrometheusMetricsClient) formatLabels(labels []metricsTypes.MetricsLabel) prometheus.Labels {
	l := make(prometheus.Labels)
	if labels == nil {
		return l
	}
	for _, label := range labels {
		l[label.Name] = label.Value
	}
	return l
}

func (pmc *PrometheusMetricsClient) findExpectedLabels(t metricsTypes.MetricsType, name string) []string {
	for _, types := range pmc.config.Metrics[t] {
		if types.Name == name {
			return types.Labels
		}
	}
	return nil
}

// hasExpectedLabels checks if any unexpected labels are present in the given labels.
func (pmc *PrometheusMetricsClient) hasUnexpectedLabels(t metricsTypes.MetricsType, name string, providedLabels []metricsTypes.MetricsLabel) error {
	expectedLabels := pmc.findExpectedLabels(t, name)
	unexpectedLabels := make([]string, 0)

	if len(expectedLabels) == 0 && len(providedLabels) > 0 {
		pmc.logger.Sugar().Warnw("Prometheus metric has no expected labels but received labels",
			zap.String("type", string(t)),
			zap.String("name", name),
			zap.Strings("providedLabels", utils.Map(providedLabels, func(label metricsTypes.MetricsLabel, i uint64) string {
				return label.Name
			})),
		)
		return fmt.Errorf("no expected labels, received '%s'", strings.Join(expectedLabels, ", "))
	}

	for _, label := range providedLabels {
		if !slices.Contains(expectedLabels, label.Name) {
			unexpectedLabels = append(unexpectedLabels, label.Name)
		}
	}

	if len(unexpectedLabels) > 0 {
		pmc.logger.Sugar().Warnw("Prometheus metric has unexpected labels",
			zap.String("type", string(t)),
			zap.String("name", name),
			zap.Strings("unexpectedLabels", unexpectedLabels),
		)
		return fmt.Errorf("unexpected labels: '%s'", strings.Join(unexpectedLabels, ", "))
	}
	return nil
}

func (pmc *PrometheusMetricsClient) Incr(name string, labels []metricsTypes.MetricsLabel, value float64) error {
	m, ok := pmc.counters[name]
	if !ok {
		pmc.logger.Sugar().Warnw("Prometheus incr not found",
			zap.String("name", name),
		)
		return nil
	}
	if err := pmc.hasUnexpectedLabels(metricsTypes.MetricsType_Incr, name, labels); err != nil {
		return err
	}
	m.With(pmc.formatLabels(labels)).Add(value)
	return nil
}

func (pmc *PrometheusMetricsClient) Gauge(name string, value float64, labels []metricsTypes.MetricsLabel) error {
	m, ok := pmc.gauges[name]
	if !ok {
		pmc.logger.Sugar().Warnw("Prometheus guage not found",
			zap.String("name", name),
		)
		return nil
	}
	if err := pmc.hasUnexpectedLabels(metricsTypes.MetricsType_Gauge, name, labels); err != nil {
		return err
	}
	m.With(pmc.formatLabels(labels)).Set(value)
	return nil
}

func (pmc *PrometheusMetricsClient) Timing(name string, value time.Duration, labels []metricsTypes.MetricsLabel) error {
	return pmc.Histogram(name, value, labels)
}

func (pmc *PrometheusMetricsClient) Histogram(name string, value time.Duration, labels []metricsTypes.MetricsLabel) error {
	m, ok := pmc.histograms[name]
	if !ok {
		pmc.logger.Sugar().Warnw("Prometheus histogram not found",
			zap.String("name", name),
		)
		return nil
	}
	if err := pmc.hasUnexpectedLabels(metricsTypes.MetricsType_Timing, name, labels); err != nil {
		return err
	}
	m.With(pmc.formatLabels(labels)).Observe(float64(value.Milliseconds()))
	return nil
}

func (pmc *PrometheusMetricsClient) Flush() {
	// No flush needed for Prometheus
}
