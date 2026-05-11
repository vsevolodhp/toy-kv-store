package memtable

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
	wal       *WAL
	lastSSTID int
}

func New() (*Memtable, error) {
	wal, err := initWal("wal.log")
	if err != nil {
		return nil, err
	}

	lastSSTID, err := getLastSSTID()
	if err != nil {
		return nil, fmt.Errorf("unable to get SST ID: %w", err)
	}

	d := make(map[string]string, MaxSize)

	err = wal.ReplayLog(func(logOp LogOp) {
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
		data:      d,
		wal:       wal,
		lastSSTID: lastSSTID,
	}

	if len(d) == MaxSize {
		err = mt.flush()
		if err != nil {
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

	err := mt.wal.Log(LogOp{Op: "put", Key: key, Value: value})
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

	// TODO: the lastSSTID in memtable and MANIFEST can become inconsistent
	for i := mt.lastSSTID; i >= 1; i-- {
		sstName := getSSTName(i)

		b, err := os.ReadFile(sstName)
		if err != nil {
			return "", fmt.Errorf("cannot read SST table: %w", err)
		}

		// the file contains 2000 entries and has size ~59kb (for test inputs)
		// for now won't use Decoder for simplicity reasons
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

	err := mt.wal.Log(LogOp{Op: "delete", Key: key, Value: ""})
	if err != nil {
		return err
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	// TODO: delete from sst tables as well
	delete(mt.data, key)
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
	// use current dir for both MANIFEST and SST
	dir, err := os.Open(".")
	if err != nil {
		return fmt.Errorf("unable to open parent dir: %w", err)
	}
	defer dir.Close()

	if err = os.WriteFile(sstName, sst, 0644); err != nil {
		return fmt.Errorf("unable to flush: %w", err)
	}

	if err = dir.Sync(); err != nil {
		return fmt.Errorf("unable to sync parent dir: %w", err)
	}

	manifest, err := os.ReadFile("MANIFEST")
	if err != nil {
		return fmt.Errorf("unable to open MANIFEST: %w", err)
	}
	manifest = slices.Concat(manifest, []byte(sstName+"\n"))

	tmpManifest, err := os.OpenFile("MANIFEST.tmp", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("unable to create temp MANIFEST file: %w", err)
	}
	defer func() {
		if tmpManifest != nil {
			// if error happened before and we didn't manually close
			// we will still try to call Close and remove tmp file
			tmpManifest.Close()
			os.Remove("MANIFEST.tmp")
		}
	}()

	if _, err = tmpManifest.Write(manifest); err != nil {
		return fmt.Errorf("unable to write to temp MANIFEST: %w", err)
	}
	if err = tmpManifest.Sync(); err != nil {
		return fmt.Errorf("unable to sync temp MANIFEST: %w", err)
	}
	if err = tmpManifest.Close(); err != nil {
		return fmt.Errorf("unable to close MANIFEST.tmp: %w", err)
	}
	tmpManifest = nil

	if err = os.Rename("MANIFEST.tmp", "MANIFEST"); err != nil {
		return fmt.Errorf("unable to rename MANIFEST.tmp: %w", err)
	}
	if err = dir.Sync(); err != nil {
		return fmt.Errorf("unable to sync parent dir: %w", err)
	}

	mt.lastSSTID++
	clear(mt.data)

	if err = mt.wal.Truncate(); err != nil {
		return err
	}

	return nil
}

func (m *Memtable) Close() {
	m.wal.Close()
}

func getSSTName(id int) string {
	return fmt.Sprintf(SSTNameFmt, id)
}

// Reads MANIFEST and returns last SST ID
func getLastSSTID() (int, error) {
	f, err := os.OpenFile("MANIFEST", os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return 0, fmt.Errorf("unable to read MANIFEST: %w", err)
	}
	defer f.Close()

	// TODO: should I be doing at all?
	scanner := bufio.NewScanner(f)
	var lastID int
	for scanner.Scan() {
		line := scanner.Text()
		if _, err = fmt.Sscanf(line, SSTNameFmt, &lastID); err != nil {
			return 0, fmt.Errorf("cannot parse sst id: %w", err)
		}
	}
	return lastID, nil
}
