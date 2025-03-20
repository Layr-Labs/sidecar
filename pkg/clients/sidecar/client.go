package sidecar

import (
	"crypto/tls"
	eventsV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/events"
	protocolV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/protocol"
	rewardsV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/rewards"
	rpcV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/sidecar"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"math"
	"strings"
)

type SidecarClient struct {
	RewardsClient  rewardsV1.RewardsClient
	RpcClient      rpcV1.RpcClient
	ProtocolClient protocolV1.ProtocolClient
	EventsClient   eventsV1.EventsClient
}

func NewSidecarClient(url string, insecureConn bool) (*SidecarClient, error) {
	rc, err := NewSidecarRewardsClient(url, insecureConn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create rewards client")
	}

	rpcc, err := NewSidecarRpcClient(url, insecureConn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create rpc client")
	}

	pc, err := NewSidecarProtocolClient(url, insecureConn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create protocol client")
	}

	ec, err := NewSidecarEventsClient(url, insecureConn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create events client")
	}

	return &SidecarClient{
		RewardsClient:  rc,
		RpcClient:      rpcc,
		ProtocolClient: pc,
		EventsClient:   ec,
	}, nil
}

func newGrpcClient(url string, insecureConn bool) (*grpc.ClientConn, error) {
	var creds grpc.DialOption
	if strings.Contains(url, "localhost:") || strings.Contains(url, "127.0.0.1:") || insecureConn {
		creds = grpc.WithTransportCredentials(insecure.NewCredentials())
	} else {
		creds = grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: false}))
	}

	opts := []grpc.DialOption{
		creds,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(math.MaxInt32)),
	}

	return grpc.NewClient(url, opts...)
}

func NewSidecarRewardsClient(url string, insecureConn bool) (rewardsV1.RewardsClient, error) {
	grpcClient, err := newGrpcClient(url, insecureConn)
	if err != nil {
		return nil, err
	}
	return rewardsV1.NewRewardsClient(grpcClient), nil
}

func NewSidecarRpcClient(url string, insecureConn bool) (rpcV1.RpcClient, error) {
	grpcClient, err := newGrpcClient(url, insecureConn)
	if err != nil {
		return nil, err
	}
	return rpcV1.NewRpcClient(grpcClient), nil
}

func NewSidecarProtocolClient(url string, insecureConn bool) (protocolV1.ProtocolClient, error) {
	grpcClient, err := newGrpcClient(url, insecureConn)
	if err != nil {
		return nil, err
	}
	return protocolV1.NewProtocolClient(grpcClient), nil
}

func NewSidecarEventsClient(url string, insecureConn bool) (eventsV1.EventsClient, error) {
	grpcClient, err := newGrpcClient(url, insecureConn)
	if err != nil {
		return nil, err
	}
	return eventsV1.NewEventsClient(grpcClient), nil
}
