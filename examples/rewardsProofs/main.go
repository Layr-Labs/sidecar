package main

import (
	"context"
	"crypto/tls"
	"fmt"
	v1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"strings"
)

func NewSidecarClient(url string, insecureConn bool) (v1.RewardsClient, error) {
	var creds grpc.DialOption
	if strings.Contains(url, "localhost:") || strings.Contains(url, "127.0.0.1:") || insecureConn {
		creds = grpc.WithTransportCredentials(insecure.NewCredentials())
	} else {
		creds = grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: false}))
	}

	grpcClient, err := grpc.NewClient(url, creds)
	if err != nil {
		return nil, err
	}

	return v1.NewRewardsClient(grpcClient), nil
}

func main() {
	earnerAddress := "0x111116fe4f8c2f83e3eb2318f090557b7cd0bf76"
	tokens := []string{"0xdeeeeE2b48C121e6728ed95c860e296177849932"}

	client, err := NewSidecarClient("localhost:7100", true)
	if err != nil {
		log.Fatal(err)
	}

	res, err := client.GenerateClaimProof(context.Background(), &v1.GenerateClaimProofRequest{
		EarnerAddress: earnerAddress,
		Tokens:        tokens,
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Proof: %+v\n", res.Proof)
}
