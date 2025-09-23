package rpcServer

import (
	"context"
	"errors"

	pdsV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/pds/v1"
)

// GetDailyOperatorStrategyAprs returns the daily APR for all strategies for a given operator
func (rpc *RpcServer) GetDailyOperatorStrategyAprs(
	ctx context.Context,
	request *pdsV1.GetDailyOperatorStrategyAprsRequest,
) (*pdsV1.GetDailyOperatorStrategyAprsResponse, error) {
	// Only allow this endpoint to work on mainnet
	if rpc.globalConfig.Chain != "mainnet" {
		return nil, errors.New("this endpoint is only available on mainnet")
	}

	operatorAddress := request.GetOperatorAddress()
	date := request.GetDate()

	if operatorAddress == "" {
		return nil, errors.New("operator address is required")
	}
	if date == "" {
		return nil, errors.New("date is required")
	}

	aprs, err := rpc.aprDataService.GetDailyOperatorStrategyAprs(ctx, operatorAddress, date)
	if err != nil {
		return nil, err
	}

	responseAprs := make([]*pdsV1.OperatorStrategyApr, 0, len(aprs))
	for _, apr := range aprs {
		responseAprs = append(responseAprs, &pdsV1.OperatorStrategyApr{
			Strategy: apr.Strategy,
			Apr:      apr.Apr,
		})
	}

	return &pdsV1.GetDailyOperatorStrategyAprsResponse{
		Aprs: responseAprs,
	}, nil
}

// GetDailyAprForEarnerStrategy returns the daily APR for a specific earner and strategy
func (rpc *RpcServer) GetDailyAprForEarnerStrategy(
	ctx context.Context,
	request *pdsV1.GetDailyAprForEarnerStrategyRequest,
) (*pdsV1.GetDailyAprForEarnerStrategyResponse, error) {
	earnerAddress := request.GetEarnerAddress()
	strategy := request.GetStrategy()
	date := request.GetDate()

	if earnerAddress == "" {
		return nil, errors.New("earner address is required")
	}
	if strategy == "" {
		return nil, errors.New("strategy is required")
	}
	if date == "" {
		return nil, errors.New("date is required")
	}

	apr, err := rpc.aprDataService.GetDailyAprForEarnerStrategy(ctx, earnerAddress, strategy, date)
	if err != nil {
		return nil, err
	}

	return &pdsV1.GetDailyAprForEarnerStrategyResponse{
		Apr: apr,
	}, nil
}
