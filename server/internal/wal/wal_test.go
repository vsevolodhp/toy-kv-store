package wal

import (
	"path/filepath"
	"reflect"
	"testing"
)

func setupWAL(t *testing.T) *WAL {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "wal.log")
	wal, err := Open(path)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	t.Cleanup(func() {
		if err := wal.Close(); err != nil {
			t.Errorf("failed to close log file gracefully: %v", err)
		}
	})
	if wal == nil {
		t.Fatalf("WAL must not be nil")
	}
	return wal
}

func TestWAL(t *testing.T) {
	w := setupWAL(t)

	rec := Record{Op: "put", Key: "abc", Value: "abc"}
	if err := w.Append(rec); err != nil {
		t.Errorf("append failed: %v\n\t data: %+v", err, rec)
	}

	var replayed []Record
	writeReplayed := func(r Record) {
		replayed = append(replayed, r)
	}
	if err := w.Replay(writeReplayed); err != nil {
		t.Errorf("failed to read appended record: %v", err)
	}
	if len(replayed) == 0 {
		t.Errorf("replay bypasses passed callback")
	}
	if len(replayed) != 1 {
		t.Fatalf("expected exactly %d record replayed, got: %d", 1, len(replayed))
	}
	if !reflect.DeepEqual(replayed[0], rec) {
		t.Errorf("replayed record doesn't match: \n\t want: %+v \n\t got: %+v", rec, replayed[0])
	}
	if err := w.Truncate(); err != nil {
		t.Errorf("failed to truncate WAL: %v", err)
	}

	replayed = nil
	if err := w.Replay(writeReplayed); err != nil {
		t.Errorf("failed to read appended record: %v", err)
	}
	if len(replayed) != 0 {
		t.Errorf("truncate must clear WAL, got %d", len(replayed))
	}

	replayed = nil
	if err := w.Append(rec); err != nil {
		t.Errorf("append after truncate failed: %v\n\t data: %+v", err, rec)
	}
	if err := w.Replay(writeReplayed); err != nil {
		t.Errorf("failed to read appended record: %v", err)
	}
	if len(replayed) != 1 {
		t.Fatalf("expected exactly %d record replayed, got: %d", 1, len(replayed))
	}
	if !reflect.DeepEqual(replayed[0], rec) {
		t.Errorf("replayed record doesn't match: \n\t want: %+v \n\t got: %+v", rec, replayed[0])
	}
}
