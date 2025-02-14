package rpcServer

import (
	"context"
	"errors"
	"fmt"
	eventsV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/events"
	healthV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/health"
	protocolV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/protocol"
	rewardsV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/rewards"
	sidecarV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/sidecar"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	sidecarClient "github.com/Layr-Labs/sidecar/pkg/clients/sidecar"
	"github.com/Layr-Labs/sidecar/pkg/eventBus/eventBusTypes"
	"github.com/Layr-Labs/sidecar/pkg/eventFilter"
	"github.com/Layr-Labs/sidecar/pkg/eventFilter/eventTypeRegistry"
	"github.com/Layr-Labs/sidecar/pkg/proofs"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"github.com/Layr-Labs/sidecar/pkg/rewardsCalculatorQueue"
	"github.com/Layr-Labs/sidecar/pkg/service/protocolDataService"
	"github.com/Layr-Labs/sidecar/pkg/service/rewardsDataService"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rs/cors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"net/http"
	"regexp"
)

type RpcServerConfig struct {
	GrpcPort int
	HttpPort int
}

type RpcServer struct {
	Logger              *zap.Logger
	rpcConfig           *RpcServerConfig
	blockStore          storage.BlockStore
	rewardsCalculator   *rewards.RewardsCalculator
	rewardsQueue        *rewardsCalculatorQueue.RewardsCalculatorQueue
	eventBus            eventBusTypes.IEventBus
	rewardsProofs       *proofs.RewardsProofsStore
	protocolDataService *protocolDataService.ProtocolDataService
	rewardsDataService  *rewardsDataService.RewardsDataService
	globalConfig        *config.Config
	sidecarClient       *sidecarClient.SidecarClient
	filterRegistry      *eventFilter.FilterableRegistry
}

func NewRpcServer(
	config *RpcServerConfig,
	bs storage.BlockStore,
	rc *rewards.RewardsCalculator,
	rcq *rewardsCalculatorQueue.RewardsCalculatorQueue,
	eb eventBusTypes.IEventBus,
	rp *proofs.RewardsProofsStore,
	pds *protocolDataService.ProtocolDataService,
	rds *rewardsDataService.RewardsDataService,
	scc *sidecarClient.SidecarClient,
	l *zap.Logger,
	cfg *config.Config,
) (*RpcServer, error) {
	server := &RpcServer{
		rpcConfig:           config,
		blockStore:          bs,
		rewardsCalculator:   rc,
		rewardsQueue:        rcq,
		eventBus:            eb,
		rewardsProofs:       rp,
		protocolDataService: pds,
		rewardsDataService:  rds,
		Logger:              l,
		globalConfig:        cfg,
		sidecarClient:       scc,
	}

	reg, err := eventTypeRegistry.BuildFilterableEventRegistry()
	if err != nil {
		l.Sugar().Errorw("Failed to build filterable event registry", zap.Error(err))
		return nil, err
	}
	server.filterRegistry = reg

	return server, nil
}

func (s *RpcServer) registerHandlers(ctx context.Context, grpcServer *grpc.Server, mux *runtime.ServeMux) error {
	healthV1.RegisterHealthServer(grpcServer, s)
	if err := healthV1.RegisterHealthHandlerServer(ctx, mux, s); err != nil {
		s.Logger.Sugar().Errorw("Failed to register Health server", zap.Error(err))
		return err
	}

	sidecarV1.RegisterRpcServer(grpcServer, s)
	if err := sidecarV1.RegisterRpcHandlerServer(ctx, mux, s); err != nil {
		s.Logger.Sugar().Errorw("Failed to register SidecarRpc server", zap.Error(err))
		return err
	}

	rewardsV1.RegisterRewardsServer(grpcServer, s)
	if err := rewardsV1.RegisterRewardsHandlerServer(ctx, mux, s); err != nil {
		s.Logger.Sugar().Errorw("Failed to register Rewards server", zap.Error(err))
		return err
	}

	protocolV1.RegisterProtocolServer(grpcServer, s)
	if err := protocolV1.RegisterProtocolHandlerServer(ctx, mux, s); err != nil {
		s.Logger.Sugar().Errorw("Failed to register Rewards server", zap.Error(err))
		return err
	}

	eventsV1.RegisterEventsServer(grpcServer, s)
	if err := eventsV1.RegisterEventsHandlerServer(ctx, mux, s); err != nil {
		s.Logger.Sugar().Errorw("Failed to register Events server", zap.Error(err))
		return err
	}

	return nil
}

func (s *RpcServer) Start(ctx context.Context, shutdown chan bool) error {
	ctx, cancelCtx := context.WithCancel(ctx)
	grpcPort := s.rpcConfig.GrpcPort
	httpPort := s.rpcConfig.HttpPort

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

	grpcLis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		s.Logger.Sugar().Errorw("Failed to listen on grpc port",
			zap.Int("port", grpcPort),
			zap.Error(err),
		)
		cancelCtx()
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

	if err = s.registerHandlers(ctx, grpcServer, mux); err != nil {
		s.Logger.Sugar().Errorw("Failed to register handlers", zap.Error(err))
		cancelCtx()
		return err
	}

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", httpPort),
		Handler: cors.AllowAll().Handler(logger.HttpLoggerMiddleware(mux, s.Logger)),
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
		for range shutdown {
			s.Logger.Sugar().Info("Shutting down servers")
			grpcServer.GracefulStop()
			err := httpServer.Shutdown(ctx)
			if err != nil {
				s.Logger.Sugar().Errorw("Failed to shutdown http server", zap.Error(err))
			}
		}
	}()
	return nil
}

func isHealthCheckRoute(fullMethodName string) bool {
	r := regexp.MustCompile(`HealthCheck$`)

	return r.MatchString(fullMethodName)
}
