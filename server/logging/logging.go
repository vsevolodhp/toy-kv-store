package logging

import (
	"log/slog"
	"net/http"
	"time"
)

type LogMiddleware struct {
	handler http.Handler
}

func (l *LogMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s := time.Now()
	l.handler.ServeHTTP(w, r)
	slog.Info("processed", slog.Duration("elapsedTime", time.Since(s)))
}

func NewLogger(h http.Handler) *LogMiddleware {
	return &LogMiddleware{h}
}
