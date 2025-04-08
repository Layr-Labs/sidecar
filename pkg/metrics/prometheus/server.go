package prometheus

import (
	"context"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"net/http"
)

type PrometheusServerConfig struct {
	Port int
}

type PrometheusServer struct {
	config *PrometheusServerConfig
	logger *zap.Logger
}

func NewPrometheusServer(cfg *PrometheusServerConfig, l *zap.Logger) *PrometheusServer {
	return &PrometheusServer{
		config: cfg,
		logger: l,
	}
}

func (ps *PrometheusServer) Start(gracefulShutdown chan bool) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", ps.config.Port),
		Handler: mux,
	}

	go func() {
		for range gracefulShutdown {
			ps.logger.Sugar().Info("Shutting down prometheus server")
			err := httpServer.Shutdown(context.Background())
			if err != nil {
				ps.logger.Sugar().Errorw("Failed to shutdown prometheus server", zap.Error(err))
			}
		}
	}()
	go func() {
		ps.logger.Sugar().Infow("Starting prometheus server", zap.Int("port", ps.config.Port))
		if err := httpServer.ListenAndServe(); err != nil {
			ps.logger.Sugar().Fatal("Failed to start prometheus server", zap.Error(err))
		}
	}()
	return nil
}
