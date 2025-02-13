package rpcServer

import (
	"context"
	"encoding/json"
	"fmt"
	v1EigenState "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/eigenState"
	v1EthereumTypes "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/ethereumTypes"
	v1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/events"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eventBus/eventBusTypes"
	"github.com/Layr-Labs/sidecar/pkg/eventFilter"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
)

func (rpc *RpcServer) subscribeToBlocks(
	ctx context.Context,
	requestId string,
	handleBlock func(interface{}) error,
) error {
	consumer := &eventBusTypes.Consumer{
		Id:      eventBusTypes.ConsumerId(requestId),
		Context: ctx,
		Channel: make(chan *eventBusTypes.Event),
	}
	rpc.eventBus.Subscribe(consumer)
	defer rpc.eventBus.Unsubscribe(consumer)

	for {
		select {
		case <-ctx.Done():
			rpc.Logger.Sugar().Info("Context done, exiting subscription", zap.String("requestId", requestId))
			return nil
		case event := <-consumer.Channel:
			if event.Name == eventBusTypes.Event_BlockProcessed {
				if err := handleBlock(event.Data); err != nil {
					return err
				}
			}
		}
	}
}

func filterEigenStateChanges(changes map[string][]interface{}, filter *eventFilter.And, filterRegistry *eventFilter.FilterableRegistry) (map[string][]interface{}, error) {
	for modelName, modelChanges := range changes {
		for i, change := range modelChanges {
			match, err := filter.Evaluate(change, filterRegistry)
			if err != nil {
				return nil, err
			}
			if !match {
				// remove the item from the list
				changes[modelName] = append(changes[modelName][:i], changes[modelName][i+1:]...)
			}
		}
	}
	return changes, nil
}

func (rpc *RpcServer) StreamEigenStateChanges(request *v1.StreamEigenStateChangesRequest, g grpc.ServerStreamingServer[v1.StreamEigenStateChangesResponse]) error {
	// Since this rpc sidecar is not processing blocks, we need to connect to the primary sidecar to get the events
	if !rpc.globalConfig.SidecarPrimaryConfig.IsPrimary {
		ctx := g.Context()
		stream, err := rpc.sidecarClient.EventsClient.StreamEigenStateChanges(ctx, request)
		if err != nil {
			return err
		}

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if err := g.Send(resp); err != nil {
				return err
			}
		}
	}

	requestId, err := uuid.NewRandom()
	if err != nil {
		rpc.Logger.Error("Failed to generate request ID", zap.Error(err))
		return err
	}

	var filter *eventFilter.And
	filterString := request.GetStateChangeFilter()
	if filterString != "" {
		err := json.Unmarshal([]byte(filterString), &filter)
		if err != nil {
			rpc.Logger.Sugar().Errorw("Failed to unmarshal filter",
				zap.Error(err),
			)
			return err
		}
	}

	err = rpc.subscribeToBlocks(g.Context(), requestId.String(), func(data interface{}) error {
		blockProcessedData := data.(*eventBusTypes.BlockProcessedData)

		if filter != nil {
			blockProcessedData.CommittedState, err = filterEigenStateChanges(blockProcessedData.CommittedState, filter, rpc.filterRegistry)
			if err != nil {
				return err
			}
		}
		changes, err := rpc.parseCommittedChanges(blockProcessedData.CommittedState)
		if err != nil {
			return err
		}
		return g.SendMsg(&v1.StreamEigenStateChangesResponse{
			BlockNumber: blockProcessedData.Block.Number,
			StateRoot:   convertStateRootToEventTypeStateRoot(blockProcessedData.StateRoot),
			Changes:     changes,
		})
	})
	return err
}

func (rpc *RpcServer) StreamIndexedBlocks(request *v1.StreamIndexedBlocksRequest, g grpc.ServerStreamingServer[v1.StreamIndexedBlocksResponse]) error {
	// Since this rpc sidecar is not processing blocks, we need to connect to the primary sidecar to get the events
	if !rpc.globalConfig.SidecarPrimaryConfig.IsPrimary {
		ctx := g.Context()
		stream, err := rpc.sidecarClient.EventsClient.StreamIndexedBlocks(ctx, request)
		if err != nil {
			return err
		}

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if err := g.Send(resp); err != nil {
				return err
			}
		}
	}
	requestId, err := uuid.NewRandom()
	if err != nil {
		rpc.Logger.Error("Failed to generate request ID", zap.Error(err))
		return err
	}
	onlyBlocksWithData := request.GetOnlyBlocksWithData()

	var stateChangesFilter *eventFilter.And
	var blockFilter *eventFilter.And

	filters := request.GetFilters()
	if filters != nil {
		stateChangeFilterStr := filters.GetStateChangeFilter()
		if stateChangeFilterStr != "" {
			err := json.Unmarshal([]byte(stateChangeFilterStr), &stateChangesFilter)
			if err != nil {
				rpc.Logger.Sugar().Errorw("Failed to unmarshal state changes filter",
					zap.Error(err),
				)
				return err
			}
		}
		blockFilterStr := filters.GetBlockFilter()
		if blockFilterStr != "" {
			err := json.Unmarshal([]byte(blockFilterStr), &blockFilter)
			if err != nil {
				rpc.Logger.Sugar().Errorw("Failed to unmarshal block filter",
					zap.Error(err),
				)
				return err
			}
		}
	}

	err = rpc.subscribeToBlocks(g.Context(), requestId.String(), func(data interface{}) error {
		rpc.Logger.Debug("Received block", zap.Any("data", data))
		blockProcessedData := data.(*eventBusTypes.BlockProcessedData)

		if (onlyBlocksWithData && processedBlockHasData(blockProcessedData)) || !onlyBlocksWithData {
			if stateChangesFilter != nil {
				blockProcessedData.CommittedState, err = filterEigenStateChanges(blockProcessedData.CommittedState, stateChangesFilter, rpc.filterRegistry)
				if err != nil {
					return err
				}
			}
			fmt.Printf("blockProcessedData.Block.Number: %v\n", blockProcessedData.Block.Number)

			resp, err := rpc.buildBlockResponse(blockProcessedData, request.GetIncludeStateChanges())
			if err != nil {
				return err
			}

			return g.SendMsg(resp)
		}
		return nil
	})
	return err
}

func processedBlockHasData(block *eventBusTypes.BlockProcessedData) bool {
	return len(block.Transactions) > 0 || len(block.Logs) > 0 || len(block.CommittedState) > 0
}

func convertTransactionLogToEventTypeTransaction(log *storage.TransactionLog) *v1EthereumTypes.TransactionLog {
	return &v1EthereumTypes.TransactionLog{
		TransactionHash:  log.TransactionHash,
		TransactionIndex: log.TransactionIndex,
		LogIndex:         log.LogIndex,
		BlockNumber:      log.BlockNumber,
		Address:          log.Address,
		EventName:        log.EventName,
	}
}

func convertTransactionToEventTypeTransaction(tx *storage.Transaction) *v1EthereumTypes.Transaction {
	return &v1EthereumTypes.Transaction{
		TransactionHash:  tx.TransactionHash,
		TransactionIndex: tx.TransactionIndex,
		BlockNumber:      tx.BlockNumber,
		FromAddress:      tx.FromAddress,
		ToAddress:        tx.ToAddress,
		ContractAddress:  tx.ContractAddress,
		Logs:             nil,
	}
}

func convertBlockToEventTypeBlock(block *storage.Block) *v1EthereumTypes.Block {
	return &v1EthereumTypes.Block{
		BlockNumber: block.Number,
		BlockHash:   block.Hash,
		BlockTime:   timestamppb.New(block.BlockTime),
		ParentHash:  block.ParentHash,
	}
}

func convertStateRootToEventTypeStateRoot(stateRoot *stateManager.StateRoot) *v1EigenState.StateRoot {
	return &v1EigenState.StateRoot{
		EthBlockNumber: stateRoot.EthBlockNumber,
		EthBlockHash:   stateRoot.EthBlockHash,
		StateRoot:      stateRoot.StateRoot,
	}
}

func convertBlockDataToEventTypes(blockData *eventBusTypes.BlockProcessedData) *v1EthereumTypes.Block {
	block := convertBlockToEventTypeBlock(blockData.Block)

	transactions := make([]*v1EthereumTypes.Transaction, 0)
	for _, tx := range blockData.Transactions {
		transaction := convertTransactionToEventTypeTransaction(tx)

		transactionLogs := make([]*v1EthereumTypes.TransactionLog, 0)
		for _, log := range blockData.Logs {
			if log.TransactionHash == tx.TransactionHash {
				transaction.Logs = append(transaction.Logs, convertTransactionLogToEventTypeTransaction(log))
			}
		}
		transaction.Logs = transactionLogs
		transactions = append(transactions, transaction)
	}
	block.Transactions = transactions
	return block
}

func (rpc *RpcServer) buildBlockResponse(blockData *eventBusTypes.BlockProcessedData, includeStateChanges bool) (*v1.StreamIndexedBlocksResponse, error) {
	resp := &v1.StreamIndexedBlocksResponse{
		Block:     convertBlockDataToEventTypes(blockData),
		StateRoot: convertStateRootToEventTypeStateRoot(blockData.StateRoot),
	}
	if includeStateChanges {
		changes, err := rpc.parseCommittedChanges(blockData.CommittedState)
		if err != nil {
			return nil, err
		}
		resp.Changes = changes
	}
	return resp, nil
}
