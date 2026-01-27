package memtable

import (
	"errors"
	"log/slog"
	"sync"
)

var ErrKeyNotFound = errors.New("key not found")
var ErrEmptyKey = errors.New("empty key")

type Memtable struct {
	mu   sync.RWMutex
	data map[string]string
}

func New() *Memtable {
	return &Memtable{
		data: make(map[string]string),
	}
}

func (mt *Memtable) Put(key, value string) error {
	if key == "" {
		return ErrEmptyKey
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	if _, ok := mt.data[key]; ok {
		slog.Debug("updating value", slog.String("key", key))
	}
	mt.data[key] = value

	return nil
}

func (mt *Memtable) Get(key string) (string, error) {
	if key == "" {
		return "", ErrEmptyKey
	}

	mt.mu.RLock()
	defer mt.mu.RUnlock()

	v, ok := mt.data[key]
	if !ok {
		slog.Debug("key does not exist", slog.String("key", key))
		return "", ErrKeyNotFound
	}
	return v, nil
}

func (mt *Memtable) Delete(key string) error {
	if key == "" {
		return ErrEmptyKey
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	delete(mt.data, key)

	return nil
}
