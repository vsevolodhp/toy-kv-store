package memtable

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
)

// TODO:
// - Implement LRU Negative Cache (no disk scans for repeatedly non-existent keys)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrEmptyKey    = errors.New("empty key")
)

const (
	MaxSize    = 2_000
	SSTNameFmt = "sst-%d.json"
)

type Entry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Memtable struct {
	mu        sync.RWMutex
	data      map[string]string
	lastSSTID int
}

func New() (*Memtable, error) {
	f, err := os.OpenFile("MANIFEST", os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("unable to read MANIFEST: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var lastID int
	for scanner.Scan() {
		line := scanner.Text()
		if _, err = fmt.Sscanf(line, SSTNameFmt, &lastID); err != nil {
			return nil, fmt.Errorf("cannot parse sst id: %w", err)
		}
	}

	memtable := &Memtable{
		data:      make(map[string]string, MaxSize),
		lastSSTID: lastID,
	}

	err = memtable.replayWAL()
	if err != nil {
		return nil, err
	}

	return memtable, nil
}

func (mt *Memtable) Put(key, value string) error {
	if key == "" {
		return ErrEmptyKey
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	err := writeToWAL("put", key, value)
	if err != nil {
		return err
	}

	mt.data[key] = value

	if len(mt.data) == MaxSize {
		err = mt.flush()
		if err != nil {
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

	for i := mt.lastSSTID; i >= 1; i-- {
		sstName := getSSTName(i)

		b, err := os.ReadFile(sstName)
		if err != nil {
			return "", fmt.Errorf("cannot read SST table: %w", err)
		}

		entries := make([]Entry, 0, MaxSize)
		err = json.Unmarshal(b, &entries)
		if err != nil {
			return "", fmt.Errorf("cannot unmarshal sst table content: %w", err)
		}

		for _, e := range entries {
			if e.Key == key {
				return e.Value, nil
			}
		}
	}

	return "", ErrKeyNotFound
}

func (mt *Memtable) Delete(key string) error {
	if key == "" {
		return ErrEmptyKey
	}

	err := writeToWAL("delete", key, "")
	if err != nil {
		return err
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	delete(mt.data, key)
	return nil
}

type walEntry struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (mt *Memtable) replayWAL() error {
	f, err := os.OpenFile("wal.db", os.O_CREATE|os.O_RDONLY, 0666)
	if err != nil {
		return fmt.Errorf("unable to open WAL: %w", err)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	for {
		var entry walEntry
		err = dec.Decode(&entry)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("unable to process WAL: %w", err)
		}

		switch entry.Op {
		case "put":
			mt.data[entry.Key] = entry.Value
		case "delete":
			delete(mt.data, entry.Key)
		default:
			return fmt.Errorf("unknown op: %s", entry.Op)
		}
	}
	return nil
}

func writeToWAL(op, key, value string) error {
	f, err := os.OpenFile("wal.db", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("unable to open WAL: %w", err)
	}
	defer f.Close()
	e := walEntry{
		Op:    op,
		Key:   key,
		Value: value,
	}
	b, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("unable to marshal: %w", err)
	}
	_, err = f.WriteString(string(b) + "\n")
	if err != nil {
		return fmt.Errorf("unable to write to WAL: %w", err)
	}
	_ = f.Sync()

	return nil
}

func (mt *Memtable) flush() error {
	entries := make([]Entry, 0, MaxSize)

	for k, v := range mt.data {
		entries = append(entries, Entry{k, v})
	}
	slices.SortFunc(entries, func(a, b Entry) int {
		return strings.Compare(a.Key, b.Key)
	})

	sst, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("unable to marshal: %w", err)
	}

	sstName := getSSTName(mt.lastSSTID + 1)
	err = os.WriteFile(sstName, sst, 0644)
	if err != nil {
		return fmt.Errorf("unable to flush: %w", err)
	}

	manifest, err := os.OpenFile("MANIFEST", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("unable to open MANIFEST: %w", err)
	}
	defer manifest.Close()

	_, err = manifest.WriteString(sstName + "\n")
	if err != nil {
		return fmt.Errorf("unable to write to MANIFEST: %w", err)
	}

	mt.lastSSTID++
	clear(mt.data)

	err = os.Truncate("wal.db", 0)
	if err != nil {
		return fmt.Errorf("unable to truncate WAL: %w", err)
	}

	return nil
}

func getSSTName(id int) string {
	return fmt.Sprintf(SSTNameFmt, id)
}
