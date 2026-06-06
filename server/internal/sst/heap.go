package sst

import (
	"container/heap"
	"strings"
)

type HeapItem struct {
	Entry    TableEntry
	TableIdx int
}

type MinHeap struct {
	data []HeapItem
}

func NewHeap(n int) *MinHeap {
	return &MinHeap{make([]HeapItem, 0, n)}
}

func (h *MinHeap) PushEntry(entry TableEntry, tableIdx int) {
	item := HeapItem{entry, tableIdx}
	heap.Push(h, item)
}

func (h *MinHeap) PopEntry() HeapItem {
	return heap.Pop(h).(HeapItem)
}

func (h *MinHeap) Push(x any) {
	e := x.(HeapItem)
	h.data = append(h.data, e)
}

func (h *MinHeap) Pop() any {
	n := len(h.data)
	item := h.data[n-1]
	h.data = h.data[0 : n-1]
	return item
}

func (h *MinHeap) Less(i, j int) bool {
	res := strings.Compare(h.data[i].Entry.Key, h.data[j].Entry.Key)
	// non-zero res means keys are not equal
	if res != 0 {
		return res == -1
	}

	// if they're equal, prioritize value from newer table
	return h.data[i].TableIdx > h.data[j].TableIdx
}

func (h *MinHeap) Len() int {
	return len(h.data)
}

func (h *MinHeap) Swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
}
