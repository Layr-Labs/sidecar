package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"strings"

	v1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/events"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func NewSidecarClient(url string, insecureConn bool) (v1.EventsClient, error) {
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

	return v1.NewEventsClient(grpcClient), nil
}

func streamIndexedBlocks(client v1.EventsClient) {
	stream, err := client.StreamIndexedBlocks(context.Background(), &v1.StreamIndexedBlocksRequest{})
	if err != nil {
		panic(err)
	}

	for {
		resp := &v1.StreamIndexedBlocksResponse{}
		err := stream.RecvMsg(resp)
		if err == io.EOF {
			fmt.Printf("Server has finished sending\n")
			return
		}
		if err != nil {
			log.Fatalf("failed to receive: %v", err)
		}

		fmt.Printf("Received: %v\n", resp)
	}
}

func streamStateChanges(client v1.EventsClient) {
	stream, err := client.StreamEigenStateChanges(context.Background(), &v1.StreamEigenStateChangesRequest{})
	if err != nil {
		panic(err)
	}

	for {
		resp := &v1.StreamEigenStateChangesResponse{}
		err := stream.RecvMsg(resp)
		if err == io.EOF {
			fmt.Printf("Server has finished sending\n")
			return
		}
		if err != nil {
			log.Fatalf("failed to receive: %v", err)
		}

		fmt.Printf("Received: %v\n", resp)
	}
}

func main() {
	client, err := NewSidecarClient("localhost:7100", true)
	if err != nil {
		panic(err)
	}

	streamStateChanges(client)
}
