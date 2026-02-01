package logging

import (
	"log/slog"
	"net/http"
	"time"
)

type Middleware struct {
	handler http.Handler
}

func (l *Middleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s := time.Now()
	l.handler.ServeHTTP(w, r)
	slog.Info("processed",
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.Duration("elapsedTime", time.Since(s)))
}

func NewMiddleware(h http.Handler) *Middleware {
	return &Middleware{h}
}
