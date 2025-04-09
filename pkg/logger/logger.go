package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"net/http"
	"regexp"
	"time"
)

type LoggerConfig struct {
	Debug bool
}

func NewLogger(cfg *LoggerConfig, options ...zap.Option) (*zap.Logger, error) {
	mergedOptions := []zap.Option{
		zap.WithCaller(true),
	}
	copy(mergedOptions, options)

	c := zap.NewProductionConfig()
	c.EncoderConfig = zap.NewProductionEncoderConfig()
	c.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	if cfg.Debug {
		c.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	} else {
		c.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	return c.Build(mergedOptions...)
}

func HttpLoggerMiddleware(next http.Handler, l *zap.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		healthRegex := regexp.MustCompile(`v1\/health$`)
		readyRegex := regexp.MustCompile(`v1\/ready$`)

		if !healthRegex.MatchString(r.URL.Path) && !readyRegex.MatchString(r.URL.Path) {
			l.Sugar().Infow("http_request",
				zap.String("system", "http"),
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Duration("duration", time.Since(start)),
			)
		}
	})
}
