package rpcServer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/Layr-Labs/sidecar/internal/version"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"

	sidecarV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/sidecar"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ContractLoadParams struct {
	Address          string
	Abi              string
	BytecodeHash     string
	BlockNumber      uint64
	AssociateToProxy string
}

func requestToLoadParams(req *sidecarV1.LoadContractRequest) contractManager.ContractLoadParams {
	return contractManager.ContractLoadParams{
		Address:          req.GetAddress(),
		Abi:              req.GetAbi(),
		BytecodeHash:     req.GetBytecodeHash(),
		BlockNumber:      req.GetBlockNumber(),
		AssociateToProxy: req.GetAssociateToProxy(),
	}
}

func (rpc *RpcServer) GetBlockHeight(ctx context.Context, req *sidecarV1.GetBlockHeightRequest) (*sidecarV1.GetBlockHeightResponse, error) {
	verified := req.GetVerified()
	block, err := rpc.protocolDataService.GetCurrentBlockHeight(ctx, verified)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if block == nil {
		return nil, status.Error(codes.NotFound, "Block not found")
	}
	return &sidecarV1.GetBlockHeightResponse{
		BlockNumber: block.Number,
		BlockHash:   block.Hash,
	}, nil
}

func (rpc *RpcServer) GetStateRoot(ctx context.Context, req *sidecarV1.GetStateRootRequest) (*sidecarV1.GetStateRootResponse, error) {
	blockNumber := req.GetBlockNumber()
	stateRoot, err := rpc.protocolDataService.GetStateRoot(ctx, blockNumber)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &sidecarV1.GetStateRootResponse{
		EthBlockHash:   stateRoot.EthBlockHash,
		EthBlockNumber: stateRoot.EthBlockNumber,
		StateRoot:      stateRoot.StateRoot,
	}, nil
}

func (rpc *RpcServer) About(ctx context.Context, req *sidecarV1.AboutRequest) (*sidecarV1.AboutResponse, error) {
	return &sidecarV1.AboutResponse{
		Version: version.GetVersion(),
		Commit:  version.GetCommit(),
		Chain:   rpc.globalConfig.Chain.String(),
	}, nil
}

// LoadContract handles the gRPC request to load a contract into the contract store
func (rpc *RpcServer) LoadContract(ctx context.Context, req *sidecarV1.LoadContractRequest) (*sidecarV1.LoadContractResponse, error) {
	// Load contract is a write operation so we need to proxy the request to the "primary" sidecar
	if !rpc.globalConfig.SidecarPrimaryConfig.IsPrimary {
		return rpc.sidecarClient.RpcClient.LoadContract(ctx, req)
	}

	// Convert request to domain params
	params := requestToLoadParams(req)

	contractAddress, err := rpc.contractManager.LoadContract(ctx, params)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to load contract: %v", err))
	}

	// Get current block height for response
	block, err := rpc.protocolDataService.GetCurrentBlockHeight(ctx, false)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &sidecarV1.LoadContractResponse{
		BlockHeight: block.Number,
		Address:     contractAddress,
	}, nil
}

func (rpc *RpcServer) LoadContracts(ctx context.Context, req *sidecarV1.LoadContractsRequest) (*sidecarV1.LoadContractsResponse, error) {
	// Load contracts is a write operation so we need to proxy the request to the "primary" sidecar
	if !rpc.globalConfig.SidecarPrimaryConfig.IsPrimary {
		return rpc.sidecarClient.RpcClient.LoadContracts(ctx, req)
	}

	reader, loadedAddresses, err := requestToReaderAndAddresses(req)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to prepare contract data: %v", err))
	}

	err = rpc.contractStore.InitializeExternalContractsFromReader(reader)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to initialize external contracts: %v", err))
	}

	// Get current block height for response
	block, err := rpc.protocolDataService.GetCurrentBlockHeight(ctx, false)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &sidecarV1.LoadContractsResponse{
		BlockHeight: block.Number,
		Addresses:   loadedAddresses,
	}, nil
}

// requestToReaderAndAddresses converts a LoadContractsRequest to an io.Reader containing the JSON representation
// of contractsData and a list of contract addresses.
func requestToReaderAndAddresses(req *sidecarV1.LoadContractsRequest) (io.Reader, []string, error) {
	contractAddresses := make([]string, 0, len(req.GetCoreContracts()))

	contractsData := &contractStore.CoreContractsData{
		CoreContracts:  make([]contractStore.CoreContract, 0, len(req.GetCoreContracts())),
		ProxyContracts: make([]contractStore.CoreProxyContract, 0, len(req.GetProxyContracts())),
	}

	for _, core := range req.GetCoreContracts() {
		contractsData.CoreContracts = append(contractsData.CoreContracts, contractStore.CoreContract{
			ContractAddress: core.GetContractAddress(),
			ContractAbi:     core.GetContractAbi(),
			BytecodeHash:    core.GetBytecodeHash(),
		})
		contractAddresses = append(contractAddresses, core.GetContractAddress())
	}

	for _, proxy := range req.GetProxyContracts() {
		contractsData.ProxyContracts = append(contractsData.ProxyContracts, contractStore.CoreProxyContract{
			ContractAddress:      proxy.GetContractAddress(),
			ProxyContractAddress: proxy.GetProxyContractAddress(),
			BlockNumber:          int64(proxy.GetBlockNumber()),
		})
	}

	jsonData, err := json.Marshal(contractsData)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal contract data: %w", err)
	}

	return bytes.NewReader(jsonData), contractAddresses, nil
}
