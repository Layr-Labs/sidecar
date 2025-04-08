package dogstatsd

import (
	"fmt"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/Layr-Labs/sidecar/pkg/metrics/metricsTypes"
	"go.uber.org/zap"
)

type DogStatsdMetricsClient struct {
	client     *statsd.Client
	logger     *zap.Logger
	sampleRate float64
}

func NewDogStatsdMetricsClient(addr string, sampleRate float64, l *zap.Logger) (*DogStatsdMetricsClient, error) {
	var err error
	s, err := statsd.New(addr,
		statsd.WithNamespace("sidecar."),
		statsd.WithBufferFlushInterval(time.Second*2),
	)

	if err != nil {
		l.Sugar().Errorw("Failed to create dogstatsd metrics client", zap.Error(err))
		return nil, err
	}

	return &DogStatsdMetricsClient{
		client:     s,
		logger:     l,
		sampleRate: sampleRate,
	}, nil
}

func (s *DogStatsdMetricsClient) formatLabels(labels []metricsTypes.MetricsLabel) []string {
	tags := make([]string, 0, len(labels))
	for _, label := range labels {
		tags = append(tags, fmt.Sprintf("%s:%s", label.Name, label.Value))
	}
	return tags
}

func (s *DogStatsdMetricsClient) Incr(name string, labels []metricsTypes.MetricsLabel, value float64) error {
	return s.client.Incr(name, s.formatLabels(labels), value)
}

func (s *DogStatsdMetricsClient) Gauge(name string, value float64, labels []metricsTypes.MetricsLabel) error {
	return s.client.Gauge(name, value, s.formatLabels(labels), s.sampleRate)
}

func (s *DogStatsdMetricsClient) Timing(name string, value time.Duration, labels []metricsTypes.MetricsLabel) error {
	return s.client.Timing(name, value, s.formatLabels(labels), s.sampleRate)
}

func (s *DogStatsdMetricsClient) Flush() {
	if err := s.client.Flush(); err != nil {
		s.logger.Sugar().Errorw("Failed to flush dogstatsd metrics client", zap.Error(err))
	}
}
