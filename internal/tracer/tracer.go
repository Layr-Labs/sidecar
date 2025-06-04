package tracer

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/mocktracer"
	ddTracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

// StartTracer initializes the DataDog tracer
// If enabled is false, it starts a mock tracer instead
func StartTracer(enabled bool, chain config.Chain) {
	if !enabled {
		mocktracer.Start()
		return
	}
	ddTracer.Start(
		ddTracer.WithEnv(string(chain)),
		ddTracer.WithServiceName("sidecar"),
		ddTracer.WithGlobalServiceName(true),
		ddTracer.WithDebugMode(false),
		ddTracer.WithLogStartup(false),
	)
}
