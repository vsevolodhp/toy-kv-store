package memtable 

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type LogOp struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type WAL struct {
	file    *os.File
	encoder *json.Encoder
}

func initWal(filename string) (*WAL, error) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to init WAL: %w", err)
	}
	return &WAL{
		file:    f,
		encoder: json.NewEncoder(f),
	}, nil
}

func (wal *WAL) Log(op LogOp) error {
	// encode is buffered, could crash mid-write
	if err := wal.encoder.Encode(op); err != nil {
		return fmt.Errorf("unable to write WAL op: %w", err)
	}

	if err := wal.file.Sync(); err != nil {
		return fmt.Errorf("unable to fsync: %w", err)
	}

	return nil
}

// Replays log entries one by one
func (wal *WAL) ReplayLog(applyFn func(LogOp)) error {
	_, err := wal.file.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("unable to seek to the beginning of the file: %w", err)
	}

	dec := json.NewDecoder(wal.file)
	for {
		var op LogOp
		err := dec.Decode(&op)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("unable to decode a log op: %w", err)
		}

		applyFn(op)
	}

	_, err = wal.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("unable to seek to the end of the file: %w", err)
	}

	return nil
}

func (wal *WAL) Truncate() error {
	// TODO: another way would be atomic update
	if err := wal.file.Truncate(0); err != nil {
		return fmt.Errorf("unable to truncate WAL: %w", err)
	}
	if _, err := wal.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("unable to update I/O offset: %w", err)
	}
	if err := wal.file.Sync(); err != nil {
		return fmt.Errorf("unable to fsync: %w", err)
	}
	return nil
}

func (wal *WAL) Close() error {
	return wal.file.Close()
}
