package sst_test

import (
	"reflect"
	"testing"

	"github.com/vsevolodhp/toy-kv-store/server/internal/sst"
)

func TestHeap(t *testing.T) {
	tests := []struct {
		name     string
		input    []sst.HeapItem
		expected []sst.HeapItem
	}{
		{
			name: "pop returns element with the lowest key",
			input: []sst.HeapItem{
				{Entry: sst.TableEntry{Key: "def", Value: "def"}, TableIdx: 1},
				{Entry: sst.TableEntry{Key: "abc", Value: "abc"}, TableIdx: 0},
				{Entry: sst.TableEntry{Key: "a", Value: "a"}, TableIdx: 2},
			},
			expected: []sst.HeapItem{
				{Entry: sst.TableEntry{Key: "a", Value: "a"}, TableIdx: 2},
				{Entry: sst.TableEntry{Key: "abc", Value: "abc"}, TableIdx: 0},
				{Entry: sst.TableEntry{Key: "def", Value: "def"}, TableIdx: 1},
			},
		},
		{
			name: "if keys are equal, prioritize newer table (larger TableIdx)",
			input: []sst.HeapItem{
				{Entry: sst.TableEntry{Key: "abc", Value: "old"}, TableIdx: 1},
				{Entry: sst.TableEntry{Key: "abc", Value: "new"}, TableIdx: 3},
				{Entry: sst.TableEntry{Key: "abc", Value: "mid"}, TableIdx: 2},
			},
			expected: []sst.HeapItem{
				{Entry: sst.TableEntry{Key: "abc", Value: "new"}, TableIdx: 3},
				{Entry: sst.TableEntry{Key: "abc", Value: "mid"}, TableIdx: 2},
				{Entry: sst.TableEntry{Key: "abc", Value: "old"}, TableIdx: 1},
			},
		},
		{
			name: "mixed keys and identical keys with different table indices",
			input: []sst.HeapItem{
				{Entry: sst.TableEntry{Key: "xyz", Value: "xyz-1"}, TableIdx: 1},
				{Entry: sst.TableEntry{Key: "abc", Value: "abc-2"}, TableIdx: 2},
				{Entry: sst.TableEntry{Key: "abc", Value: "abc-4"}, TableIdx: 4},
				{Entry: sst.TableEntry{Key: "mno", Value: "mno-0"}, TableIdx: 0},
			},
			expected: []sst.HeapItem{
				{Entry: sst.TableEntry{Key: "abc", Value: "abc-4"}, TableIdx: 4},
				{Entry: sst.TableEntry{Key: "abc", Value: "abc-2"}, TableIdx: 2},
				{Entry: sst.TableEntry{Key: "mno", Value: "mno-0"}, TableIdx: 0},
				{Entry: sst.TableEntry{Key: "xyz", Value: "xyz-1"}, TableIdx: 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := sst.NewHeap(len(tt.input))
			for _, item := range tt.input {
				h.PushEntry(item.Entry, item.TableIdx)
			}

			for i, want := range tt.expected {
				if h.Len() == 0 {
					t.Fatalf("pop at %d: expected %q, but heap is empty", i, want.Entry.Key)
				}
				got := h.PopEntry()
				if !reflect.DeepEqual(got, want) {
					t.Errorf("pop at %d:\n \twant: %+v\n \tgot: %+v", i, want, got)
				}
			}

			if h.Len() != 0 {
				t.Errorf("heap must be drained, but it has %d entries", h.Len())
			}
		})
	}
}
