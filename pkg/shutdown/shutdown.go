// Package shutdown provides utilities for handling graceful application shutdown.
// It includes functions for setting up signal handlers and coordinating the shutdown process.
package shutdown

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
)

// CreateGracefulShutdownChannel creates and returns a channel that will receive
// termination signals (SIGTERM, SIGINT). This channel can be used to detect when
// the application should begin its shutdown process.
//
// Returns:
//   - chan os.Signal: A channel that will receive OS termination signals
func CreateGracefulShutdownChannel() chan os.Signal {
	gracefulShutdown := make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM, syscall.SIGINT)

	return gracefulShutdown
}

// ListenForShutdown blocks until a termination signal is received, then executes
// the provided signal handler function and waits for the specified duration before
// signaling completion.
//
// Parameters:
//   - signalChan: Channel to receive termination signals from
//   - done: Channel to close when shutdown is complete
//   - signalHandler: Function to execute when a termination signal is received
//   - timeToWait: Duration to wait after executing the handler before signaling completion
//   - l: Logger for recording shutdown events
func ListenForShutdown(
	signalChan chan os.Signal,
	done chan bool,
	signalHandler func(),
	timeToWait time.Duration,
	l *zap.Logger,
) {
	sig := <-signalChan
	switch sig {
	case syscall.SIGTERM, syscall.SIGINT:
		l.Sugar().Infof("caught signal %v", sig)

		signalHandler()

		l.Sugar().Infof("Waiting %v seconds to exit...", timeToWait.Seconds())
		time.Sleep(timeToWait)

		l.Sugar().Infof("Exiting")
		close(done)
	}
}
