package rpcServer

import (
	"context"
	"testing"

	rewardsV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/rewards"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/rewardsCalculatorQueue"
	"github.com/stretchr/testify/assert"
)

// TestGenerateRewards_ContextCanceled_NoPanic reproduces the crash that took
// the sidecar down on 2026-07-29 03:09 UTC.
//
// When a gRPC client cancels its context mid-generation (e.g. the client pod
// hits its k8s activeDeadlineSeconds), rewardsQueue.EnqueueAndWait returns
// (nil, ctx.Err()). Before the fix, GenerateRewards read data.CutoffDate
// unconditionally, causing a nil-pointer dereference (SIGSEGV) that crashed
// the whole sidecar process.
//
// The queue is intentionally NOT started (no Process() goroutine), so the
// enqueued message is never picked up. The buffered channel absorbs the write,
// then EnqueueAndWait's select immediately fires on ctx.Done().
func TestGenerateRewards_ContextCanceled_NoPanic(t *testing.T) {
	l, err := logger.NewLogger(&logger.LoggerConfig{Debug: false})
	assert.NoError(t, err)

	// Queue with a nil calculator is safe here — the ctx.Done() path never
	// touches the calculator. Process() is deliberately not started.
	queue := rewardsCalculatorQueue.NewRewardsCalculatorQueue(nil, l)

	rpc := &RpcServer{
		Logger:       l,
		rewardsQueue: queue,
		globalConfig: &config.Config{
			SidecarPrimaryConfig: config.SidecarPrimaryConfig{IsPrimary: true},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel BEFORE the call, so EnqueueAndWait sees ctx.Done() immediately

	assert.NotPanics(t, func() {
		resp, err := rpc.GenerateRewards(ctx, &rewardsV1.GenerateRewardsRequest{
			CutoffDate:      "2026-07-28",
			WaitForComplete: true,
		})
		// With the fix, we should get a clean error return (canceled context
		// bubbles up as codes.Internal). Response payload is not meaningful here;
		// we only care that we didn't panic.
		assert.Error(t, err)
		_ = resp
	})
}
