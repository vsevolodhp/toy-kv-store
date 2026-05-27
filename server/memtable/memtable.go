package memtable

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/vsevolodhp/toy-kv-store/server/internal/sst"
	"github.com/vsevolodhp/toy-kv-store/server/internal/wal"
)

// TODO:
// - Implement LRU Negative Cache (no disk scans for repeatedly non-existent keys)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrEmptyKey    = errors.New("empty key")
)

const (
	MaxSize    = 200 // TODO: upd to 2k later
	SSTNameFmt = "sst-%d.json"
)

type Entry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Memtable struct {
	mu         sync.RWMutex
	data       map[string]string
	wal        *wal.WAL
	sstManager *sst.Manager
}

func New() (*Memtable, error) {
	w, err := wal.Open("wal.log")
	if err != nil {
		return nil, err
	}

	sstMngr, err := sst.New()
	if err != nil {
		return nil, err
	}

	d := make(map[string]string, MaxSize)

	err = w.Replay(func(logOp wal.Record) {
		switch logOp.Op {
		case "put":
			d[logOp.Key] = logOp.Value
		case "delete":
			delete(d, logOp.Key)
		default:
			slog.Error("unspported log operation", slog.String("operation", logOp.Op))
		}
	})

	if err != nil {
		return nil, fmt.Errorf("unable to replay WAL: %w", err)
	}

	mt := &Memtable{
		data:       d,
		wal:        w,
		sstManager: sstMngr,
	}

	if len(d) == MaxSize {
		if err = mt.flush(); err != nil {
			return nil, err
		}
	}

	return mt, nil
}

func (mt *Memtable) Put(key, value string) error {
	if key == "" {
		return ErrEmptyKey
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	if err := mt.wal.Append(wal.Record{Op: "put", Key: key, Value: value}); err != nil {
		return err
	}

	mt.data[key] = value

	if len(mt.data) == MaxSize {
		if err := mt.flush(); err != nil {
			return err
		}
	}

	return nil
}

func (mt *Memtable) Get(key string) (string, error) {
	if key == "" {
		return "", ErrEmptyKey
	}

	mt.mu.RLock()
	defer mt.mu.RUnlock()

	v, ok := mt.data[key]
	if ok {
		return v, nil
	}

	// TODO: search in sst
	return "", ErrKeyNotFound
}

func (mt *Memtable) Delete(key string) error {
	if key == "" {
		return ErrEmptyKey
	}

	if err := mt.wal.Append(wal.Record{Op: "delete", Key: key, Value: ""}); err != nil {
		return err
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	// TODO: delete from sst tables as well
	delete(mt.data, key)
	return nil
}

func (mt *Memtable) flush() error {
	entries := make([]sst.TableEntry, 0, MaxSize)
	for k, v := range mt.data {
		entries = append(entries, sst.TableEntry{
			Key:   k,
			Value: v,
		})
	}

	if err := mt.sstManager.Write(entries); err != nil {
		return err
	}

	clear(mt.data)

	return mt.wal.Truncate()
}

func (m *Memtable) Close() {
	m.wal.Close()
}
