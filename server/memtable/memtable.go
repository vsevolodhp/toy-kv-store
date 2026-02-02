package memtable

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrEmptyKey    = errors.New("empty key")
)

const MaxSize = 2_000

type Entry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Memtable struct {
	mu        sync.RWMutex
	data      map[string]string
	size      int
	lastSSTID int
}

func New() (*Memtable, error) {
	f, err := os.OpenFile("MANIFEST", os.O_CREATE|os.O_RDONLY, 0666)
	if err != nil {
		return nil, fmt.Errorf("unable to read MANIFEST: %w", err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	var lastID int
	for scanner.Scan() {
		line := scanner.Text()
		strs := strings.Split(line, "-")
		id, err := strconv.Atoi(strings.TrimSuffix(strs[1], ".json"))
		if err != nil {
			return nil, fmt.Errorf("cannot parse sst id: %w", err)
		}
		lastID = id
	}
	return &Memtable{
		data:      make(map[string]string, MaxSize),
		lastSSTID: lastID,
	}, nil
}

func (mt *Memtable) Put(key, value string) error {
	if key == "" {
		return ErrEmptyKey
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	if _, ok := mt.data[key]; ok {
		slog.Debug("updating value", slog.String("key", key))
	} else {
		mt.size++
	}
	mt.data[key] = value

	if mt.size == MaxSize {
		entries := make([]Entry, 0, MaxSize)

		for k, v := range mt.data {
			entries = append(entries, Entry{k, v})
		}
		slices.SortFunc(entries, func(a, b Entry) int {
			return strings.Compare(a.Key, b.Key)
		})

		flush, err := json.Marshal(entries)
		if err != nil {
			return fmt.Errorf("unable to marshal: %w", err)
		}

		sstName := fmt.Sprintf("sst-%d.json", mt.lastSSTID+1)
		err = os.WriteFile(sstName, flush, 0666)
		if err != nil {
			return fmt.Errorf("unable to flush: %w", err)
		}

		manifest, err := os.OpenFile("MANIFEST", os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("unable to open MANIFEST: %w", err)
		}

		_, err = manifest.WriteString(sstName + "\n")
		manifest.Close()
		if err != nil {
			return fmt.Errorf("unable to write to MANIFEST: %w", err)
		}

		mt.lastSSTID++
		mt.size = 0
		clear(mt.data)
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
