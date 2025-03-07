package main

import (
	"context"
	"crypto/tls"
	"github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/rewards"
	"github.com/akuity/grpc-gateway-client/pkg/grpc/gateway"
	"math"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"strings"
)

func NewGrpcSidecarClient(url string, insecureConn bool) (rewards.RewardsClient, error) {
	var creds grpc.DialOption
	if strings.Contains(url, "localhost:") || strings.Contains(url, "127.0.0.1:") || insecureConn {
		creds = grpc.WithTransportCredentials(insecure.NewCredentials())
	} else {
		creds = grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: false}))
	}

	opts := []grpc.DialOption{
		creds,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
	}

	grpcClient, err := grpc.NewClient(url, opts...)
	if err != nil {
		return nil, err
	}

	return rewards.NewRewardsClient(grpcClient), nil
}

func NewHttpSidecarClient(url string, opts ...gateway.ClientOption) rewards.RewardsGatewayClient {
	return rewards.NewRewardsGatewayClient(gateway.NewClient(url, opts...))
}

func grpcExample() {
	client, err := NewGrpcSidecarClient("localhost:7100", true)
	if err != nil {
		panic(err)
	}

	distributionRoots, err := client.ListDistributionRoots(context.Background(), &rewards.ListDistributionRootsRequest{})
	if err != nil {
		panic(err)
	}

	rewardsData, err := client.GetRewardsForDistributionRoot(context.Background(), &rewards.GetRewardsForDistributionRootRequest{
		RootIndex: distributionRoots.DistributionRoots[0].RootIndex,
	})
	if err != nil {
		panic(err)
	}

	for _, reward := range rewardsData.Rewards {
		println(reward.String())
	}
}

func httpExample() {
	client := NewHttpSidecarClient("http://localhost:7101")

	distributionRoots, err := client.ListDistributionRoots(context.Background(), &rewards.ListDistributionRootsRequest{})
	if err != nil {
		panic(err)
	}

	rewardsData, err := client.GetRewardsForDistributionRoot(context.Background(), &rewards.GetRewardsForDistributionRootRequest{
		RootIndex: distributionRoots.DistributionRoots[0].RootIndex,
	})
	if err != nil {
		panic(err)
	}

	for _, reward := range rewardsData.Rewards {
		println(reward.String())
	}
}

func main() {
	grpcExample()
	httpExample()
}
