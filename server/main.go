package main

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"

	"github.com/vsevolodhp/toy-kv-store/server/logging"
)

type server struct {
	mux sync.RWMutex
	kv  map[string]string
}

func newServer() *server {
	return &server{
		kv: make(map[string]string),
	}
}

func main() {
	slog.Info("starting toy-kv server")
	s := newServer()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{key}", s.handleGet())
	mux.HandleFunc("PUT /{key}", s.handlePut())
	mux.HandleFunc("DELETE /{key}", s.handleDelete())

	wrapped := logging.NewLogger(mux)
	_ = http.ListenAndServe(":8080", wrapped)
}

func (s *server) handleGet() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if key == "" {
			http.Error(w, "key cannot be empty", http.StatusBadRequest)
			return
		}

		s.mux.RLock()
		defer s.mux.RUnlock()

		v, ok := s.kv[key]
		if !ok {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte(v))
	}
}

func (s *server) handlePut() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if key == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		b, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "unable to read body", http.StatusBadRequest)
			return
		}

		s.mux.Lock()
		defer s.mux.Unlock()

		v := string(b)
		if _, ok := s.kv[key]; ok {
			fmt.Println("key exist, value is going to be updated")
		}

		s.kv[key] = v
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte(v))
	}
}

func (s *server) handleDelete() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if key == "" {
			http.Error(w, "key cannot be empty", http.StatusBadRequest)
			return
		}

		s.mux.Lock()
		defer s.mux.Unlock()

		if _, ok := s.kv[key]; !ok {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}

		delete(s.kv, key)
		w.WriteHeader(http.StatusOK)
	}
}
