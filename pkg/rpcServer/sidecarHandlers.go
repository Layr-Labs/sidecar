package rpcServer

import (
	"context"
	"fmt"

	"github.com/Layr-Labs/sidecar/internal/version"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"github.com/Layr-Labs/sidecar/pkg/postgres/helpers"

	sidecarV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/sidecar"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

type ContractLoadParams struct {
	Address          string
	Abi              string
	BytecodeHash     string
	BlockNumber      uint64
	AssociateToProxy string
}

func requestToLoadParams(req *sidecarV1.LoadContractRequest) ContractLoadParams {
	return ContractLoadParams{
		Address:          req.GetAddress(),
		Abi:              req.GetAbi(),
		BytecodeHash:     req.GetBytecodeHash(),
		BlockNumber:      req.GetBlockNumber(),
		AssociateToProxy: req.GetAssociateToProxy(),
	}
}

func coreContractToLoadParams(core *sidecarV1.CoreContract) ContractLoadParams {
	return ContractLoadParams{
		Address:      core.GetContractAddress(),
		Abi:          core.GetContractAbi(),
		BytecodeHash: core.GetBytecodeHash(),
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

func (rpc *RpcServer) loadContractWithinTx(ctx context.Context, params ContractLoadParams, tx *gorm.DB) (string, error) {
	if params.Address == "" {
		return "", status.Error(codes.Internal, "contract address is required")
	}
	if params.Abi == "" {
		return "", status.Error(codes.Internal, "contract ABI is required with the contract address")
	}

	if params.BytecodeHash == "" {
		abiFetcher := rpc.protocolDataService.GetAbiFetcher()

		var err error
		params.BytecodeHash, err = abiFetcher.FetchContractBytecodeHash(ctx, params.Address)
		if err != nil {
			return "", status.Error(codes.Internal, err.Error())
		}
	}

	txContractStore := rpc.protocolDataService.GetContractStore()
	if txContractStore == nil {
		return "", status.Error(codes.Internal, "Contract store not available")
	}

	// Load the contract
	contract, err := txContractStore.CreateContract(
		params.Address,
		params.Abi,
		true,
		params.BytecodeHash,
		"",
		true,
		contractStore.ContractType_External,
	)
	if err != nil {
		return "", fmt.Errorf("failed to load contract: %w", err)
	}

	// Associate to proxy if specified
	if params.AssociateToProxy != "" {
		proxyContract, err := txContractStore.GetContractForAddress(params.AssociateToProxy)
		if err != nil {
			return "", fmt.Errorf("error checking for proxy contract: %w", err)
		}
		if proxyContract == nil {
			return "", fmt.Errorf("proxy contract %s not found", params.AssociateToProxy)
		}
		if params.BlockNumber == 0 {
			return "", fmt.Errorf("block number is required to load a proxy contract")
		}
		_, err = txContractStore.CreateProxyContract(params.BlockNumber, params.AssociateToProxy, params.Address)
		if err != nil {
			return "", fmt.Errorf("failed to load to proxy contract: %w", err)
		}
	}

	return contract.ContractAddress, nil
}

// LoadContract handles the gRPC request to load a contract into the contract store
func (rpc *RpcServer) LoadContract(ctx context.Context, req *sidecarV1.LoadContractRequest) (*sidecarV1.LoadContractResponse, error) {
	// Convert request to domain params
	params := requestToLoadParams(req)

	// Wrap contract creation and proxy association in a transaction
	contractAddress, err := helpers.WrapTxAndCommit[string](func(tx *gorm.DB) (string, error) {
		return rpc.loadContractWithinTx(ctx, params, tx)
	}, rpc.protocolDataService.GetDB(), nil)

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
	loadedAddresses := []string{}
	// Process all contracts in a single transaction
	_, err := helpers.WrapTxAndCommit[string](func(tx *gorm.DB) (string, error) {
		for _, coreContract := range req.GetCoreContracts() {
			params := coreContractToLoadParams(coreContract)

			address, err := rpc.loadContractWithinTx(ctx, params, tx)
			if err != nil {
				return "", fmt.Errorf("failed to load core contract %s: %w", address, err)
			}
			loadedAddresses = append(loadedAddresses, address)
		}

		// Get the contract store from the protocol data service
		txContractStore := rpc.protocolDataService.GetContractStore()
		if txContractStore == nil {
			return "", fmt.Errorf("Contract store not available")
		}

		// Then process all proxy associations
		for _, proxyContract := range req.GetProxyContracts() {
			contractAddr := proxyContract.GetContractAddress()
			proxyAddr := proxyContract.GetProxyContractAddress()
			blockNumber := proxyContract.GetBlockNumber()

			// Validate required parameters
			if contractAddr == "" {
				return "", fmt.Errorf("contract address is required")
			}
			if proxyAddr == "" {
				return "", fmt.Errorf("proxy contract address is required")
			}
			if blockNumber == 0 {
				return "", fmt.Errorf("block number is required to load a proxy contract")
			}

			// Create the proxy association
			_, err := txContractStore.CreateProxyContract(blockNumber, proxyAddr, contractAddr)
			if err != nil {
				return "", fmt.Errorf("failed to associate contract %s with proxy %s: %w", contractAddr, proxyAddr, err)
			}
		}

		return "", nil
	}, rpc.protocolDataService.GetDB(), nil)

	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to load contracts: %v", err))
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
