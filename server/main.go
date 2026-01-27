package main

import (
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/vsevolodhp/toy-kv-store/server/logging"
	"github.com/vsevolodhp/toy-kv-store/server/memtable"
)


func main() {
	slog.Info("starting toy-kv server")
	mt := memtable.New()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{key}", handleGet(mt))
	mux.HandleFunc("PUT /{key}", handlePut(mt))
	mux.HandleFunc("DELETE /{key}", handleDelete(mt))

	wrapped := logging.NewLogger(mux)
	_ = http.ListenAndServe(":8080", wrapped)
}

func handleGet(mt *memtable.Memtable) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		v, err := mt.Get(key)
		if errors.Is(err, memtable.ErrEmptyKey) {
			http.Error(w, "key cannot be empty", http.StatusBadRequest)
			return
		}
		if errors.Is(err, memtable.ErrKeyNotFound) {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte(v))
	}
}

func handlePut(mt *memtable.Memtable) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if key == "" {
			http.Error(w, "key cannot be empty", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()
		b, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "unable to read body", http.StatusBadRequest)
			return
		}
		v := string(b)
		_ = mt.Put(key, v)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write(b)
	}
}

func handleDelete(mt *memtable.Memtable) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		err := mt.Delete(key)
		if errors.Is(err, memtable.ErrEmptyKey) {
			http.Error(w, "key cannot be empty", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}
