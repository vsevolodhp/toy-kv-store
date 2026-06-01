package sst

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
)

const (
	manifestPath = "MANIFEST"
	TableSize    = 200
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrKeyDeleted  = errors.New("key is deleted")
)

type Manager struct {
	activeTables []string
}

type TableEntry struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Deleted bool   `json:"deleted"`
}

func New() (*Manager, error) {
	f, err := os.OpenFile(manifestPath, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("sst: failed to create MANIFEST: %w", err)
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	var tables []string
	for s.Scan() {
		tables = append(tables, s.Text())
	}
	if err := s.Err(); err != nil {
		return nil, fmt.Errorf("sst: failed to scan MANIFEST: %w", err)
	}
	return &Manager{
		activeTables: tables,
	}, nil
}

func (m *Manager) Write(entries []TableEntry) error {
	// just don't do anything if there is nothing to write
	if len(entries) == 0 {
		return nil
	}

	// sort & marshal data to store in sst table
	slices.SortFunc(entries, func(a, b TableEntry) int {
		return strings.Compare(a.Key, b.Key)
	})

	data, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("sst: failed to marshal entries: %w", err)
	}

	filename := fmt.Sprintf("sst-%d.json", len(m.activeTables))

	// write sst table to disk
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("sst: failed to create new table: %w", err)
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("sst: failed to write data: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sst: failed to fsync table: %w", err)
	}

	// sync dir
	dir, err := os.Open(".")
	if err != nil {
		return fmt.Errorf("sst: failed to open dir: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sst: failed to sync dir: %w", err)
	}

	// update manifest atomically
	tmp, err := os.OpenFile(manifestPath+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("sst: failed to create tmp MANIFEST: %w", err)
	}
	// close happens before rename, but if there were errors earlier ensure to close fd
	isTmpClosed := false
	defer func() {
		if !isTmpClosed {
			tmp.Close()
		}
	}()
	for _, v := range m.activeTables {
		if _, err := tmp.WriteString(v + "\n"); err != nil {
			return fmt.Errorf("sst: failed to copy MANIFEST to tmp file: %w", err)
		}
	}
	if _, err := tmp.Write(append([]byte(filename), '\n')); err != nil {
		return fmt.Errorf("sst: failed to write to tmp MANIFEST: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sst: failed to sync tmp MANIFEST: %w", err)
	}
	tmp.Close() // close before rename
	isTmpClosed = true
	if err := os.Rename(tmp.Name(), manifestPath); err != nil {
		return fmt.Errorf("sst: failed to update MANIFEST: %w", err)
	}
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sst: failed to sync dir: %w", err)
	}

	// if everything succeded update list of active tables
	m.activeTables = append(m.activeTables, filename)

	return nil
}

func (m *Manager) Seek(key string) (string, error) {
	if len(m.activeTables) == 0 {
		return "", ErrKeyNotFound
	}

	// new tables appended to activeTables, for search prioritize newest tables
	for _, filename := range slices.Backward(m.activeTables) {
		value, err := fileSeek(filename, key)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				// continue search in other files
				continue
			}
			return "", err
		}
		return value, nil
	}

	return "", ErrKeyNotFound
}

func fileSeek(filename, key string) (string, error) {
	d, err := os.ReadFile(filename) // sst table is small in this project (< 60KB), so read in memory for simplicity
	if err != nil {
		return "", fmt.Errorf("sst: unable to read table: %w", err)
	}

	entries := make([]TableEntry, 0, TableSize)
	if err := json.Unmarshal(d, &entries); err != nil {
		return "", fmt.Errorf("sst: failed to unmarshal table: %w", err)
	}

	idx, found := slices.BinarySearchFunc(entries, key, func(entry TableEntry, k string) int {
		return strings.Compare(entry.Key, k)
	})

	if found {
		if entries[idx].Deleted {
			return "", ErrKeyDeleted
		}
		return entries[idx].Value, nil
	}

	return "", ErrKeyNotFound
}
