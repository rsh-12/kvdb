package pagination

import (
	"container/heap"
	"kvdb/core/lsm"
	"kvdb/types"
	"log"
)

type Queue struct {
	entries     []Entry
	tombestones map[string]bool
}

type Entry struct {
	item  types.Item
	iter  types.Iterator
	index int
}

func (q *Queue) isTombstone(key string) bool {
	return q.tombestones[key]
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

func (q *Queue) Push(x any) {
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

	queue, err := initQueue(iterators)
	if err != nil {
		return nil, err
	}

	return processQueue(queue, page), nil
}

func initQueue(iterators []types.Iterator) (*Queue, error) {
	queue := &Queue{}
	queue.tombestones = map[string]bool{}
	heap.Init(queue)

	for i, it := range iterators {
		if it.HasNext() {
			item, err := it.Next()
			if err != nil {
				return nil, err
			}

			heap.Push(queue, Entry{
				item:  item,
				iter:  it,
				index: i,
			})

			if queue.isTombstone(item.Key) {
				queue.tombestones[item.Key] = true
			}
		}
	}

	return queue, nil
}

func processQueue(queue *Queue, page Page) []types.Item {
	var result []types.Item
	seenKeys := make(map[string]bool)
	count := 0

	for queue.Len() > 0 && count < page.Offset+page.Limit {
		entry := heap.Pop(queue).(Entry)
		item := entry.item

		if seenKeys[item.Key] || queue.isTombstone(item.Key) {
			pushNextItem(queue, entry)
			continue
		}

		if count >= page.Offset {
			result = append(result, item)
			seenKeys[item.Key] = true
		}
		count++

		pushNextItem(queue, entry)
	}

	return result
}

func pushNextItem(queue *Queue, entry Entry) {
	if entry.iter.HasNext() {
		item, err := entry.iter.Next()
		if err != nil {
			log.Fatal("error pushing next item:", err)
		}

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
