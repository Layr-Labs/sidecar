package sidecar

import (
	"context"
	"errors"
	"fmt"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/pipeline"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"github.com/Layr-Labs/sidecar/pkg/rewardsCalculatorQueue"
	"github.com/Layr-Labs/sidecar/pkg/rpcServer"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net"
	"net/http"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/cors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type SidecarConfig struct {
	GenesisBlockNumber uint64
}

type Sidecar struct {
	Logger                 *zap.Logger
	Config                 *SidecarConfig
	GlobalConfig           *config.Config
	Storage                storage.BlockStore
	Pipeline               *pipeline.Pipeline
	EthereumClient         *ethereum.Client
	StateManager           *stateManager.EigenStateManager
	RewardsCalculator      *rewards.RewardsCalculator
	RewardsCalculatorQueue *rewardsCalculatorQueue.RewardsCalculatorQueue
	ShutdownChan           chan bool
	shouldShutdown         *atomic.Bool
}

func NewSidecar(
	cfg *SidecarConfig,
	gCfg *config.Config,
	s storage.BlockStore,
	p *pipeline.Pipeline,
	em *stateManager.EigenStateManager,
	rc *rewards.RewardsCalculator,
	rcq *rewardsCalculatorQueue.RewardsCalculatorQueue,
	l *zap.Logger,
	ethClient *ethereum.Client,
) *Sidecar {
	shouldShutdown := &atomic.Bool{}
	shouldShutdown.Store(false)
	return &Sidecar{
		Logger:                 l,
		Config:                 cfg,
		GlobalConfig:           gCfg,
		Storage:                s,
		Pipeline:               p,
		EthereumClient:         ethClient,
		RewardsCalculator:      rc,
		RewardsCalculatorQueue: rcq,
		StateManager:           em,
		ShutdownChan:           make(chan bool),
		shouldShutdown:         shouldShutdown,
	}
}

func (s *Sidecar) Start(ctx context.Context) {
	s.Logger.Info("Starting sidecar")

	// Spin up a goroutine that listens on a channel for a shutdown signal.
	// When the signal is received, set shouldShutdown to true and return.
	go func() {
		for range s.ShutdownChan {
			s.Logger.Sugar().Infow("Received shutdown signal")
			s.shouldShutdown.Store(true)
		}
	}()

	s.StartIndexing(ctx)
	/*
		Main loop:

		- Get current indexed block
			- If no blocks, start from the genesis block
			- If some blocks, start from last indexed block
		- Once at tip, begin listening for new blocks
	*/
}

func isHealthCheckRoute(fullMethodName string) bool {
	r := regexp.MustCompile(`HealthCheck$`)

	return r.MatchString(fullMethodName)
}

func HttpLoggerMiddleware(next http.Handler, l *zap.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		healthRegex := regexp.MustCompile(`v1\/health$`)
		readyRegex := regexp.MustCompile(`v1\/ready$`)

		if !healthRegex.MatchString(r.URL.Path) && !readyRegex.MatchString(r.URL.Path) {
			l.Sugar().Infow("http_request",
				zap.String("system", "http"),
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Duration("duration", time.Since(start)),
			)
		}
	})
}

func (s *Sidecar) WithRpcServer(
	ctx context.Context,
	bs storage.BlockStore,
	sm *stateManager.EigenStateManager,
	gracefulShutdown chan bool,
) error {
	grpc_zap.ReplaceGrpcLoggerV2(s.Logger)

	opts := []grpc_zap.Option{
		grpc_zap.WithDecider(func(fullMethodName string, err error) bool {
			if err == nil && isHealthCheckRoute(fullMethodName) {
				return false
			}
			// by default everything else will be logged
			return true
		}),
	}

	grpcPort := s.GlobalConfig.RpcConfig.GrpcPort
	grpcLis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		s.Logger.Sugar().Errorw("Failed to listen", zap.Error(err), zap.Int("port", grpcPort))
		return fmt.Errorf("failed to listen: %w", err)
	}

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			grpc_ctxtags.UnaryServerInterceptor(grpc_ctxtags.WithFieldExtractor(grpc_ctxtags.CodeGenRequestFieldExtractor)),
			grpc_zap.UnaryServerInterceptor(s.Logger, opts...),
		),
	)
	reflection.Register(grpcServer)
	mux := runtime.NewServeMux()

	_, err = rpcServer.NewRpcServer(ctx, grpcServer, mux, bs, sm, s.RewardsCalculator, s.RewardsCalculatorQueue, s.Logger)
	if err != nil {
		s.Logger.Sugar().Errorw("Failed to create rpc server", zap.Error(err))
		return err
	}

	ctx, cancelCtx := context.WithCancel(ctx)
	httpPort := s.GlobalConfig.RpcConfig.HttpPort
	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", httpPort),
		Handler: cors.AllowAll().Handler(HttpLoggerMiddleware(mux, s.Logger)),
		BaseContext: func(listener net.Listener) context.Context {
			//nolint:staticcheck
			ctx = context.WithValue(ctx, "httpServer", listener.Addr().String())
			return ctx
		},
	}

	s.Logger.Sugar().Infow("Starting servers...",
		zap.Int("grpcPort", grpcPort),
		zap.Int("httpPort", httpPort),
	)
	go func() {
		s.Logger.Sugar().Infow("Starting HTTP server")
		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			s.Logger.Sugar().Infow("HTTP server closed")
		} else {
			s.Logger.Sugar().Fatal("failed to serve", zap.Error(err))
		}
		cancelCtx()
	}()
	go func() {
		if err := grpcServer.Serve(grpcLis); err != nil {
			s.Logger.Sugar().Fatal("failed to serve reload server", zap.Error(err))
		}
	}()

	go func() {
		for range gracefulShutdown {
			s.Logger.Sugar().Info("Shutting down servers")
			grpcServer.GracefulStop()
			err = httpServer.Shutdown(ctx)
			if err != nil {
				s.Logger.Sugar().Errorw("Failed to shutdown http server", zap.Error(err))
			}
		}
	}()

	return nil
}

func (s *Sidecar) WithPrometheusServer(gracefulShutdown chan bool) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", s.GlobalConfig.PrometheusConfig.Port),
		Handler: HttpLoggerMiddleware(mux, s.Logger),
	}

	go func() {
		for range gracefulShutdown {
			s.Logger.Sugar().Info("Shutting down prometheus server")
			err := httpServer.Shutdown(context.Background())
			if err != nil {
				s.Logger.Sugar().Errorw("Failed to shutdown prometheus server", zap.Error(err))
			}
		}
	}()
	go func() {
		s.Logger.Sugar().Infow("Starting prometheus server", zap.Int("port", s.GlobalConfig.PrometheusConfig.Port))
		if err := httpServer.ListenAndServe(); err != nil {
			s.Logger.Sugar().Fatal("Failed to start prometheus server", zap.Error(err))
		}
	}()
}
