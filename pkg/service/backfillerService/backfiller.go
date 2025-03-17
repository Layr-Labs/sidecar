package backfillerService

import (
	"context"
	"sync"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/service/baseDataService"
	"github.com/Layr-Labs/sidecar/pkg/transactionBackfiller"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

// Define status constants for backfill jobs
type BackfillStatus int32

const (
	BackfillStatus_QUEUED     BackfillStatus = 0
	BackfillStatus_PROCESSING BackfillStatus = 1
	BackfillStatus_COMPLETED  BackfillStatus = 2
	BackfillStatus_FAILED     BackfillStatus = 3
)

// BackfillRequest represents a request to backfill blocks
type BackfillRequest struct {
	StartBlock      uint64
	EndBlock        uint64
	Addresses       []string
	EventSignatures []string
}

// BackfillResponse represents a response to a backfill request
type BackfillResponse struct {
	Success bool
}

// BackfillStatusRequest represents a request for the status of a backfill job
type BackfillStatusRequest struct {
	RequestId string
}

// BackfillStatusResponse represents the status of a backfill job
type BackfillStatusResponse struct {
	RequestId    string
	Status       BackfillStatus
	Progress     uint32
	StartBlock   uint64
	CurrentBlock uint64
	EndBlock     uint64
	Errors       []string
}

// TransactionBackfiller_StreamBackfillStatusServer is an interface for streaming backfill status
type TransactionBackfiller_StreamBackfillStatusServer interface {
	Send(*BackfillStatusResponse) error
	Context() context.Context
}

// backfillJob represents an active backfill operation
type backfillJob struct {
	requestID    string
	request      *BackfillRequest
	status       BackfillStatus
	startTime    time.Time
	endTime      time.Time
	currentBlock uint64
	errors       []string
	statusChan   chan *BackfillStatusResponse
	doneChan     chan struct{}
}

// BackfillerService implements the TransactionBackfiller gRPC service
type BackfillerService struct {
	baseDataService.BaseDataService
	backfiller    *transactionBackfiller.TransactionBackfiller
	db            *gorm.DB
	logger        *zap.Logger
	globalConfig  *config.Config
	activeJobs    map[string]*backfillJob
	activeJobsMux sync.RWMutex
}

// NewBackfillerService creates a new BackfillerService
func NewBackfillerService(
	backfiller *transactionBackfiller.TransactionBackfiller,
	db *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) *BackfillerService {
	return &BackfillerService{
		BaseDataService: baseDataService.BaseDataService{
			DB: db,
		},
		backfiller:    backfiller,
		logger:        logger,
		globalConfig:  globalConfig,
		activeJobs:    make(map[string]*backfillJob),
		activeJobsMux: sync.RWMutex{},
	}
}

// BackfillBlocks initiates a backfill operation for a range of blocks
func (bfs *BackfillerService) BackfillBlocks(
	ctx context.Context,
	req *BackfillRequest,
) (*BackfillResponse, error) {
	bfs.logger.Sugar().Infow("Received BackfillBlocks request",
		zap.Uint64("startBlock", req.StartBlock),
		zap.Uint64("endBlock", req.EndBlock),
		zap.Any("addresses", req.Addresses),
		zap.Any("eventSignatures", req.EventSignatures),
	)

	// Validate request
	if req.StartBlock > req.EndBlock {
		return nil, status.Errorf(codes.InvalidArgument, "start_block must be less than or equal to end_block")
	}

	// Generate a unique request ID
	requestID := uuid.New().String()

	// Create a new backfill job
	job := &backfillJob{
		requestID:    requestID,
		request:      req,
		status:       BackfillStatus_QUEUED,
		startTime:    time.Now(),
		currentBlock: req.StartBlock,
		statusChan:   make(chan *BackfillStatusResponse, 100),
		doneChan:     make(chan struct{}),
	}

	// Store the job
	bfs.activeJobsMux.Lock()
	bfs.activeJobs[requestID] = job
	bfs.activeJobsMux.Unlock()

	// Process the backfill request in a goroutine
	go bfs.processBackfillJob(job)

	// Return an immediate response
	return &BackfillResponse{
		Success: true,
	}, nil
}

// GetBackfillStatus retrieves the status of a backfill operation
func (bfs *BackfillerService) GetBackfillStatus(
	ctx context.Context,
	req *BackfillStatusRequest,
) (*BackfillStatusResponse, error) {
	bfs.logger.Sugar().Infow("Received GetBackfillStatus request",
		zap.String("requestID", req.RequestId),
	)

	// Get the job
	bfs.activeJobsMux.RLock()
	job, exists := bfs.activeJobs[req.RequestId]
	bfs.activeJobsMux.RUnlock()

	if !exists {
		return nil, status.Errorf(codes.NotFound, "backfill job not found: %s", req.RequestId)
	}

	// Calculate progress
	progress := uint32(0)
	if job.request.EndBlock > job.request.StartBlock {
		progress = uint32(float64(job.currentBlock-job.request.StartBlock) / float64(job.request.EndBlock-job.request.StartBlock) * 100)
	}

	// Return the status
	return &BackfillStatusResponse{
		RequestId:    job.requestID,
		Status:       job.status,
		Progress:     progress,
		StartBlock:   job.request.StartBlock,
		CurrentBlock: job.currentBlock,
		EndBlock:     job.request.EndBlock,
		Errors:       job.errors,
	}, nil
}

// StreamBackfillStatus streams the status of a backfill operation in real-time
func (bfs *BackfillerService) StreamBackfillStatus(
	req *BackfillStatusRequest,
	stream TransactionBackfiller_StreamBackfillStatusServer,
) error {
	bfs.logger.Sugar().Infow("Received StreamBackfillStatus request",
		zap.String("requestID", req.RequestId),
	)

	// Get the job
	bfs.activeJobsMux.RLock()
	job, exists := bfs.activeJobs[req.RequestId]
	bfs.activeJobsMux.RUnlock()

	if !exists {
		return status.Errorf(codes.NotFound, "backfill job not found: %s", req.RequestId)
	}

	// Send the initial status
	initialStatus, err := bfs.GetBackfillStatus(context.Background(), req)
	if err != nil {
		return err
	}
	if err := stream.Send(initialStatus); err != nil {
		return err
	}

	// Stream status updates
	for {
		select {
		case status := <-job.statusChan:
			if err := stream.Send(status); err != nil {
				return err
			}
		case <-job.doneChan:
			// Job is complete, send final status and return
			finalStatus, err := bfs.GetBackfillStatus(context.Background(), req)
			if err != nil {
				return err
			}
			return stream.Send(finalStatus)
		case <-stream.Context().Done():
			return nil
		}
	}
}

// processBackfillJob processes a backfill job
func (bfs *BackfillerService) processBackfillJob(job *backfillJob) {
	defer func() {
		close(job.doneChan)
		// Keep the job in the map for a while for status queries
		go func() {
			time.Sleep(1 * time.Hour)
			bfs.activeJobsMux.Lock()
			delete(bfs.activeJobs, job.requestID)
			bfs.activeJobsMux.Unlock()
		}()
	}()

	// Update job status
	job.status = BackfillStatus_PROCESSING
	bfs.updateJobStatus(job)

	// Create a backfill message
	message := &transactionBackfiller.BackfillerMessage{
		StartBlock: job.request.StartBlock,
		EndBlock:   job.request.EndBlock,
		IsInterestingLog: func(log *ethereum.EthereumEventLog) bool {
			// Check if the log is from one of the requested addresses
			if len(job.request.Addresses) > 0 {
				addressMatch := false
				for _, addr := range job.request.Addresses {
					if log.Address.Value() == addr {
						addressMatch = true
						break
					}
				}
				if !addressMatch {
					return false
				}
			}

			// Check if the log has one of the requested event signatures
			if len(job.request.EventSignatures) > 0 {
				topicMatch := false
				if len(log.Topics) > 0 {
					for _, sig := range job.request.EventSignatures {
						if log.Topics[0].Value() == sig {
							topicMatch = true
							break
						}
					}
				}
				if !topicMatch {
					return false
				}
			}

			return true
		},
		TransactionLogHandler: func(block *ethereum.EthereumBlock, receipt *ethereum.EthereumTransactionReceipt, log *ethereum.EthereumEventLog) error {
			// Update current block
			job.currentBlock = block.Number.Value()
			bfs.updateJobStatus(job)
			return nil
		},
	}

	// Enqueue the backfill message and wait for completion
	response, err := bfs.backfiller.EnqueueAndWait(context.Background(), message)
	if err != nil {
		job.status = BackfillStatus_FAILED
		job.errors = append(job.errors, err.Error())
		bfs.updateJobStatus(job)
		return
	}

	// Process the response
	if len(response.Errors) > 0 {
		job.status = BackfillStatus_FAILED
		for _, err := range response.Errors {
			job.errors = append(job.errors, err.Error())
		}
	} else {
		job.status = BackfillStatus_COMPLETED
		job.currentBlock = job.request.EndBlock
	}

	job.endTime = time.Now()
	bfs.updateJobStatus(job)
}

// updateJobStatus sends a status update to the status channel
func (bfs *BackfillerService) updateJobStatus(job *backfillJob) {
	// Calculate progress
	progress := uint32(0)
	if job.request.EndBlock > job.request.StartBlock {
		progress = uint32(float64(job.currentBlock-job.request.StartBlock) / float64(job.request.EndBlock-job.request.StartBlock) * 100)
	}

	// Create status response
	status := &BackfillStatusResponse{
		RequestId:    job.requestID,
		Status:       job.status,
		Progress:     progress,
		StartBlock:   job.request.StartBlock,
		CurrentBlock: job.currentBlock,
		EndBlock:     job.request.EndBlock,
		Errors:       job.errors,
	}

	// Send status update
	select {
	case job.statusChan <- status:
		// Status sent successfully
	default:
		// Channel is full, discard the update
	}
}
