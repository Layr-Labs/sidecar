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
	slashingV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/slashing"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/metrics"
	"github.com/Layr-Labs/sidecar/internal/metrics/metricsTypes"
	sidecarClient "github.com/Layr-Labs/sidecar/pkg/clients/sidecar"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"github.com/Layr-Labs/sidecar/pkg/eventBus/eventBusTypes"
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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"
)

type RpcServerConfig struct {
	GrpcPort int
	HttpPort int
}

type RpcServer struct {
	rewardsV1.UnimplementedRewardsServer
	slashingV1.UnimplementedSlashingServer
	protocolV1.UnimplementedProtocolServer
	Logger              *zap.Logger
	rpcConfig           *RpcServerConfig
	blockStore          storage.BlockStore
	contractStore       contractStore.ContractStore
	contractManager     *contractManager.ContractManager
	rewardsCalculator   *rewards.RewardsCalculator
	rewardsQueue        *rewardsCalculatorQueue.RewardsCalculatorQueue
	eventBus            eventBusTypes.IEventBus
	rewardsProofs       *proofs.RewardsProofsStore
	protocolDataService *protocolDataService.ProtocolDataService
	rewardsDataService  *rewardsDataService.RewardsDataService
	globalConfig        *config.Config
	sidecarClient       *sidecarClient.SidecarClient
	metricsSink         *metrics.MetricsSink
}

func NewRpcServer(
	config *RpcServerConfig,
	bs storage.BlockStore,
	cs contractStore.ContractStore,
	cm *contractManager.ContractManager,
	rc *rewards.RewardsCalculator,
	rcq *rewardsCalculatorQueue.RewardsCalculatorQueue,
	eb eventBusTypes.IEventBus,
	rp *proofs.RewardsProofsStore,
	pds *protocolDataService.ProtocolDataService,
	rds *rewardsDataService.RewardsDataService,
	scc *sidecarClient.SidecarClient,
	ms *metrics.MetricsSink,
	l *zap.Logger,
	cfg *config.Config,
) *RpcServer {
	server := &RpcServer{
		rpcConfig:           config,
		blockStore:          bs,
		contractStore:       cs,
		contractManager:     cm,
		rewardsCalculator:   rc,
		rewardsQueue:        rcq,
		eventBus:            eb,
		rewardsProofs:       rp,
		protocolDataService: pds,
		rewardsDataService:  rds,
		Logger:              l,
		globalConfig:        cfg,
		sidecarClient:       scc,
		metricsSink:         ms,
	}

	return server
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

func (s *RpcServer) MetricsGrpcUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		startTime := time.Now()
		method := info.FullMethod

		res, err := handler(ctx, req)

		duration := time.Since(startTime)

		labels := []metricsTypes.MetricsLabel{
			{Name: "grpc_method", Value: method},
			{Name: "status", Value: status.Code(err).String()},
			{Name: "status_code", Value: fmt.Sprintf("%d", status.Code(err))},
			{Name: "rpc", Value: "grpc"},
			{Name: "client_source", Value: "unknown"},
			{Name: "client_type", Value: "grpc"},
		}

		_ = s.metricsSink.Incr(metricsTypes.Metric_Incr_GrpcRequest, labels, 1)
		_ = s.metricsSink.Timing(metricsTypes.Metric_Timing_GrpcDuration, duration, labels)

		return res, err
	}
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

// NewStatusRecorder creates a new statusRecorder
func NewStatusRecorder(w http.ResponseWriter) *statusRecorder {
	return &statusRecorder{
		ResponseWriter: w,
		statusCode:     http.StatusOK, // Default to 200 OK
	}
}

// WriteHeader captures the status code and writes it to the underlying ResponseWriter
func (r *statusRecorder) WriteHeader(code int) {
	if !r.written {
		r.statusCode = code
		r.written = true
		r.ResponseWriter.WriteHeader(code)
	}
}

// Write captures implicit 200 status code and writes to the underlying ResponseWriter
func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.written {
		r.statusCode = http.StatusOK
		r.written = true
	}
	return r.ResponseWriter.Write(b)
}

type RequestMetadata struct {
	Method  string
	Pattern string
}

const requestMetadataKey = "request_metadata"

func parseOriginIpFromRequest(req *http.Request) string {
	ip := req.Header.Get("X-Forwarded-For")

	// If empty, try other common headers
	if ip == "" {
		ip = req.Header.Get("X-Real-IP")
	}

	// Fallback to RemoteAddr if headers are not available
	if ip == "" {
		ip, _, _ = net.SplitHostPort(req.RemoteAddr)
	}

	netIp := net.ParseIP(ip)
	if netIp == nil {
		return ip
	}

	if v4 := netIp.To4(); v4 != nil {
		return v4.String()
	}

	return netIp.String()
}

func (s *RpcServer) MetricsAndLogsHttpHandler(next *runtime.ServeMux, l *zap.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		clientIp := parseOriginIpFromRequest(r)

		md := &RequestMetadata{}
		//nolint:staticcheck
		r = r.WithContext(context.WithValue(r.Context(), requestMetadataKey, md))

		// Overloaded response writer to capture the status code of the response
		recorder := NewStatusRecorder(w)

		next.ServeHTTP(recorder, r)

		method := r.Method
		path := r.URL.Path
		statusCode := recorder.statusCode
		pattern := "unknown"

		if md.Pattern != "" {
			pattern = md.Pattern
		}

		grpcService := ""
		grpcMethod := ""

		if md.Method != "" {
			parts := strings.Split(md.Method, "/")
			grpcService = parts[1]
			grpcMethod = parts[2]
		}

		duration := time.Since(startTime)

		clientType := "http"

		clientSource := r.Header.Get("x-sidecar-source")
		if clientSource != "" {
			clientType = "unknown"
		}

		labels := []metricsTypes.MetricsLabel{
			{Name: "method", Value: method},
			{Name: "path", Value: path},
			{Name: "status_code", Value: fmt.Sprintf("%d", statusCode)},
			{Name: "grpc_method", Value: md.Method},
			{Name: "pattern", Value: pattern},
			{Name: "rpc", Value: "http"},
			{Name: "client_ip", Value: clientIp},
			{Name: "client_source", Value: clientSource},
			{Name: "client_type", Value: clientType},
		}

		_ = s.metricsSink.Incr(metricsTypes.Metric_Incr_HttpRequest, labels, 1)
		_ = s.metricsSink.Timing(metricsTypes.Metric_Timing_HttpDuration, duration, labels)

		healthRegex := regexp.MustCompile(`v1\/health$`)
		readyRegex := regexp.MustCompile(`v1\/ready$`)

		if !healthRegex.MatchString(r.URL.Path) && !readyRegex.MatchString(path) {
			l.Sugar().Infow(fmt.Sprintf("%s %s", method, path),
				zap.String("system", "http"),
				zap.String("method", method),
				zap.String("path", path),
				zap.Int("status_code", statusCode),
				zap.Duration("duration", duration),
				zap.String("pattern", pattern),
				zap.String("grpc.service", grpcService),
				zap.String("grpc.method", grpcMethod),
				zap.Uint64("grpc.time_ms", uint64(duration.Milliseconds())),
				zap.String("client_ip", clientIp),
				zap.String("client_source", clientSource),
				zap.String("client_type", clientType),
			)
		}
	})
}

func injectGrpcHttpMetadata(ctx context.Context, r *http.Request) metadata.MD {
	requestMetadata := r.Context().Value(requestMetadataKey).(*RequestMetadata)

	md := make(map[string]string)
	if method, ok := runtime.RPCMethod(ctx); ok {
		md["method"] = method
		if requestMetadata != nil {
			requestMetadata.Method = method
		}
	}
	if pattern, ok := runtime.HTTPPathPattern(ctx); ok {
		md["pattern"] = pattern
		if requestMetadata != nil {
			requestMetadata.Pattern = pattern
		}
	}

	return metadata.New(md)
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

	mux := runtime.NewServeMux(
		runtime.WithMetadata(injectGrpcHttpMetadata),
	)

	if err = s.registerHandlers(ctx, grpcServer, mux); err != nil {
		s.Logger.Sugar().Errorw("Failed to register handlers", zap.Error(err))
		cancelCtx()
		return err
	}

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", httpPort),
		Handler: cors.AllowAll().Handler(s.MetricsAndLogsHttpHandler(mux, s.Logger)),
		BaseContext: func(listener net.Listener) context.Context {
			//nolint:staticcheck
			ctx = context.WithValue(ctx, "httpServer", listener.Addr().String())
			return ctx
		},
		WriteTimeout: time.Second * 300,
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
