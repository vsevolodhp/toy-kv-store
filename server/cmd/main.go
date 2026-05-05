package main

import (
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/vsevolodhp/toy-kv-store/server/logger"
	"github.com/vsevolodhp/toy-kv-store/server/memtable"
	"github.com/vsevolodhp/toy-kv-store/server/wal"
)

func main() {
	wal, err := wal.New("wal.db")
	if err != nil {
		panic("unable to init WAL")
	}
	defer wal.Close()

	mt, err := memtable.New(wal)
	if err != nil {
		panic("unable to create memtable")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{key}", handleGet(mt))
	mux.HandleFunc("PUT /{key}", handlePut(mt))
	mux.HandleFunc("DELETE /{key}", handleDelete(mt))

	handler := logger.WithRequestLogger(mux)

	err = http.ListenAndServe(":8080", handler)
	if err != nil {
		panic("unable to run server")
	}

}

func handleGet(mt *memtable.Memtable) http.HandlerFunc {
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

func handlePut(mt *memtable.Memtable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if key == "" {
			http.Error(w, "key cannot be empty", http.StatusBadRequest)
			return
		}

		b, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "unable to read body", http.StatusBadRequest)
			return
		}

		err = mt.Put(key, string(b))
		if err != nil {
			slog.Error("unable to put value", slog.String("key", key), slog.Any("error", err))
			http.Error(w, "unable to save value", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write(b)
	}
}

func handleDelete(mt *memtable.Memtable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")

		err := mt.Delete(key)
		if err != nil {
			if errors.Is(err, memtable.ErrEmptyKey) {
				http.Error(w, "key cannot be empty", http.StatusBadRequest)
				return
			}
			slog.Error("unable to delete", slog.String("key", key), slog.Any("error", err))
			http.Error(w, "unable to delete", http.StatusInternalServerError)
		}
	}
}
