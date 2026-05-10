package main

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"fmt"

	"github.com/vsevolodhp/toy-kv-store/server/memtable"
)

func main() {
	logOpts := &slog.HandlerOptions{Level: slog.LevelDebug}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, logOpts)))
	// TODO: pass via flags
	if err := run(8080); err != nil {
		slog.Error("unable to start server", "error", err)
		os.Exit(1)
	}
}

func run(port int) error {
	slog.Info("starting server", "port", port)
	mt, err := memtable.New()
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{key}", handleGet(mt))
	mux.HandleFunc("PUT /{key}", handlePut(mt))
	mux.HandleFunc("DELETE /{key}", handleDelete(mt))

	if err = http.ListenAndServe(fmt.Sprintf(":%d", port), mux); err != nil {
		return err
	}
	return nil
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
		if _, err = w.Write([]byte(v)); err != nil {
			slog.Error("unable to write response", "error", err)
			return
		}
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

		if err = mt.Put(key, string(b)); err != nil {
			slog.Error("unable to put value", "key", key, "error", err)
			http.Error(w, "unable to save value", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if _, err = w.Write(b); err != nil {
			slog.Error("unable to write response", "error", err)
			return
		}
	}
}

func handleDelete(mt *memtable.Memtable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if err := mt.Delete(key); err != nil {
			if errors.Is(err, memtable.ErrEmptyKey) {
				http.Error(w, "key cannot be empty", http.StatusBadRequest)
				return
			}
			slog.Error("unable to delete", "key", key, "error", err)
			http.Error(w, "unable to delete", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
