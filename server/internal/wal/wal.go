package wal

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type Record struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type WAL struct {
	// TODO: consider to use interface instead
	file *os.File
}

func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal: %w", err)
	}
	return &WAL{
		file: f, 
	}, nil
}

func (wal *WAL) Append(r Record) error {
	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("wal: failed to marshal: %w", err)
	}
	data = append(data, '\n')
	if _, err := wal.file.Write(data); err != nil {
		return fmt.Errorf("wal: failed to write: %w", err)
	}
	if err := wal.file.Sync(); err != nil {
		return fmt.Errorf("wal: failed to sync: %w", err)
	}
	return nil
}

func (wal *WAL) Replay(applyFn func(Record)) error {
	if _, err := wal.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: failed to seek start: %w", err)
	}

	d := json.NewDecoder(wal.file)
	for {
		var rec Record
		if err := d.Decode(&rec); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("wal: failed to decode: %w", err)
		}
		applyFn(rec)
	}

	if _, err := wal.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("wal: failed to seek end: %w", err)
	}
	return nil
}

func (wal *WAL) Truncate() error {
	if err := wal.file.Truncate(0); err != nil {
		return fmt.Errorf("wal: failed to truncate: %w", err)
	}
	if _, err := wal.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: failed to seek start: %w", err)
	}
	if err := wal.file.Sync(); err != nil {
		return fmt.Errorf("wal: failed to sync: %w", err)
	}
	return nil
}

func (wal *WAL) Close() error {
	return wal.file.Close()
}
