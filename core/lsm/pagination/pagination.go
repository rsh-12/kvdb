package pagination

import (
	"container/heap"
	"kvdb/core/lsm"
	"kvdb/types"
)

type Queue struct {
	entries []Entry
}

type Entry struct {
	item  types.Item
	iter  types.Iterator
	index int
}

func (q *Queue) Len() int {
	return len(q.entries)
}

func (q *Queue) Less(i, j int) bool {
	return q.entries[i].item.Key < q.entries[j].item.Key
}

func (q *Queue) Swap(i, j int) {
	q.entries[i], q.entries[j] = q.entries[j], q.entries[i]
}

func (q *Queue) Pop() any {
	old := q.entries
	n := len(old)
	entry := old[n-1]
	q.entries = old[:n-1]
	return entry
}

func (q *Queue) Push(x interface{}) {
	q.entries = append(q.entries, x.(Entry))
}

type Page struct {
	Limit  int
	Offset int
}

func Paginate(lsm *lsm.LSMTree, page Page) ([]types.Item, error) {
	iterators, err := lsm.OpenIterators()
	if err != nil {
		return nil, err
	}
	defer close(iterators)

	queue := &Queue{}
	heap.Init(queue)
	tombstones := make(map[string]bool)

	initQueue(queue, iterators, tombstones)

	var result []types.Item
	count := 0

	for queue.Len() > 0 {
		entry := heap.Pop(queue).(Entry)

		if tombstones[entry.item.Key] {
			pushNextItem(queue, entry)
			continue
		}

		if count >= page.Offset && count < page.Offset+page.Limit {
			result = append(result, entry.item)
		}
		count++

		pushNextItem(queue, entry)
	}

	clear(tombstones)
	return result, nil
}

func initQueue(queue *Queue, iterators []types.Iterator, tombstones map[string]bool) {
	for i, it := range iterators {
		if it.HasNext() {
			item, _ := it.Next()
			if item.Value == "" && !tombstones[item.Key] {
				tombstones[item.Key] = true
			}
			heap.Push(queue, Entry{
				item:  item,
				iter:  it,
				index: i,
			})
		}
	}
}

func pushNextItem(queue *Queue, entry Entry) {
	if entry.iter.HasNext() {
		item, _ := entry.iter.Next()
		heap.Push(queue, Entry{
			item:  item,
			iter:  entry.iter,
			index: entry.index,
		})
	}
}

func close(iterators []types.Iterator) {
	for _, it := range iterators {
		it.Close()
	}
}
