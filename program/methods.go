package program

import (
	"sort"
	"time"

	"github.com/keilerkonzept/topk"
	"github.com/keilerkonzept/topk/heap"
	"github.com/keilerkonzept/topk/sliding"
)

type Item struct {
	Item  string
	Count uint32
}

type Method interface {
	Name() string
	Add(item string)
	Tick()
	TopK(k int) []Item
	PlotHistory(item string, length int) []float64
}

type Naive struct{ counts map[string]uint32 }

func NewNaive() *Naive                              { return &Naive{counts: map[string]uint32{}} }
func (n *Naive) Name() string                       { return "Naive" }
func (n *Naive) Add(item string)                    { n.counts[item]++ }
func (n *Naive) Tick()                              {}
func (n *Naive) PlotHistory(string, int) []float64  { return nil }

func (n *Naive) TopK(k int) []Item {
	items := make([]Item, 0, len(n.counts))
	for s, c := range n.counts {
		items = append(items, Item{s, c})
	}
	sortItems(items)
	if len(items) > k {
		items = items[:k]
	}
	return items
}

type SlidingNaive struct {
	buckets []map[string]uint32
	head    int
	total   map[string]uint32
}

func NewSlidingNaive(windowTicks int) *SlidingNaive {
	if windowTicks < 1 {
		windowTicks = 1
	}
	bs := make([]map[string]uint32, windowTicks)
	for i := range bs {
		bs[i] = map[string]uint32{}
	}
	return &SlidingNaive{buckets: bs, total: map[string]uint32{}}
}

func (s *SlidingNaive) Name() string                      { return "Sliding Naive" }
func (s *SlidingNaive) PlotHistory(string, int) []float64 { return nil }

func (s *SlidingNaive) Add(item string) {
	s.buckets[s.head][item]++
	s.total[item]++
}

func (s *SlidingNaive) Tick() {
	s.head = (s.head + 1) % len(s.buckets)
	old := s.buckets[s.head]
	for item, c := range old {
		s.total[item] -= c
		if s.total[item] == 0 {
			delete(s.total, item)
		}
	}
	clear(old)
}

func (s *SlidingNaive) TopK(k int) []Item {
	items := make([]Item, 0, len(s.total))
	for str, c := range s.total {
		items = append(items, Item{str, c})
	}
	sortItems(items)
	if len(items) > k {
		items = items[:k]
	}
	return items
}

type Proposed struct {
	sketch *sliding.Sketch
	ranker *IncrementalRanker
	k      int
}

func NewProposed(k, windowTicks int) *Proposed {
	return &Proposed{
		sketch: sliding.New(k, windowTicks),
		ranker: NewIncrementalRanker(k, 2*time.Second, 0),
		k:      k,
	}
}

func (p *Proposed) Name() string    { return "Proposed" }
func (p *Proposed) Add(item string) { p.sketch.Incr(item) }
func (p *Proposed) Tick()           { p.sketch.Tick() }

func (p *Proposed) TopK(k int) []Item {
	items, _ := p.ranker.Refresh(time.Now(), 0,
		func() []heap.Item { return p.sketch.SortedSlice() },
		func(items []heap.Item, limit int) {
			for i := 0; i < limit; i++ {
				items[i].Count = p.sketch.Count(items[i].Item)
			}
		})
	out := make([]Item, len(items))
	for i, it := range items {
		out[i] = Item{Item: it.Item, Count: it.Count}
	}
	return out
}

func (p *Proposed) PlotHistory(item string, length int) []float64 {
	fingerprint := topk.Fingerprint(item)
	bucketIdx := make([]int, 0, p.sketch.Depth)
	for k := 0; k < p.sketch.Depth; k++ {
		idx := topk.BucketIndex(item, k, p.sketch.Width)
		b := p.sketch.Buckets[idx]
		if b.Fingerprint == fingerprint && len(b.Counts) > 0 {
			bucketIdx = append(bucketIdx, idx)
		}
	}
	series := make([]float64, length)
	if len(bucketIdx) == 0 {
		return series
	}
	for j := 0; j < length; j++ {
		var maxCount uint32
		for _, idx := range bucketIdx {
			b := p.sketch.Buckets[idx]
			c := b.Counts[(int(b.First)+j)%len(b.Counts)]
			if c > maxCount {
				maxCount = c
			}
		}
		series[length-1-j] = float64(maxCount)
	}
	return series
}

type IncrementalRanker struct {
	k               int
	fullRefresh     time.Duration
	partialSize     int
	autoBudget      int
	lastFullRefresh time.Time
	items           []heap.Item
	partialCursor   int
}

func NewIncrementalRanker(k int, fullRefresh time.Duration, partialSize int) *IncrementalRanker {
	if k < 1 {
		k = 1
	}
	if fullRefresh < 0 {
		fullRefresh = 2 * time.Second
	}
	if partialSize < 0 {
		partialSize = 0
	}
	autoBudget := k / 2
	if autoBudget < 1 {
		autoBudget = 1
	}
	if k >= 10 && autoBudget < 10 {
		autoBudget = 10
	}
	if autoBudget > 100 {
		autoBudget = 100
	}
	if autoBudget > k {
		autoBudget = k
	}
	return &IncrementalRanker{k: k, fullRefresh: fullRefresh, partialSize: partialSize, autoBudget: autoBudget}
}

func (r *IncrementalRanker) Refresh(now time.Time, budgetItems int, sortedFn func() []heap.Item, updateCountsFn func(items []heap.Item, limit int)) (items []heap.Item, didFull bool) {
	if now.IsZero() {
		now = time.Now()
	}

	needFull := len(r.items) == 0 || r.lastFullRefresh.IsZero()
	if r.fullRefresh == 0 {
		needFull = true
	} else if r.fullRefresh > 0 && now.Sub(r.lastFullRefresh) >= r.fullRefresh {
		needFull = true
	}
	if needFull {
		discovered := sortedFn()
		if len(discovered) > r.k {
			discovered = discovered[:r.k]
		}
		r.items = cloneItems(discovered)
		r.partialCursor = 0
		r.lastFullRefresh = now
		if len(r.items) == 0 {
			return nil, true
		}
		return cloneItems(r.items), true
	}
	if len(r.items) == 0 {
		return nil, false
	}

	limit := len(r.items)
	if r.partialSize > 0 {
		if r.partialSize < limit {
			limit = r.partialSize
		}
	} else {
		if budgetItems <= 0 {
			budgetItems = r.autoBudget
		}
		if budgetItems > 0 && budgetItems < limit {
			limit = budgetItems
		}
	}
	if limit <= 0 || len(r.items) == 0 {
		return cloneItems(r.items), needFull
	}

	if limit >= len(r.items) {
		updateCountsFn(r.items, len(r.items))
	} else {
		start := r.partialCursor % len(r.items)
		end := start + limit
		if end <= len(r.items) {
			seg := r.items[start:end]
			updateCountsFn(seg, len(seg))
		} else {
			segA := r.items[start:]
			segB := r.items[:end-len(r.items)]
			updateCountsFn(segA, len(segA))
			updateCountsFn(segB, len(segB))
		}
		r.partialCursor = (start + limit) % len(r.items)
	}

	insertionSort(r.items)

	end := len(r.items)
	for end > 0 && r.items[end-1].Count == 0 {
		end--
	}
	r.items = r.items[:end]
	if len(r.items) == 0 {
		r.partialCursor = 0
	}

	return cloneItems(r.items), needFull
}

func sortItems(items []Item) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].Count != items[j].Count {
			return items[i].Count > items[j].Count
		}
		return items[i].Item < items[j].Item
	})
}

func insertionSort(items []heap.Item) {
	for i := 1; i < len(items); i++ {
		x := items[i]
		j := i
		for j > 0 && less(x, items[j-1]) {
			items[j] = items[j-1]
			j--
		}
		items[j] = x
	}
}

func less(a, b heap.Item) bool {
	if a.Count != b.Count {
		return a.Count > b.Count
	}
	return a.Item < b.Item
}

func cloneItems(in []heap.Item) []heap.Item {
	out := make([]heap.Item, len(in))
	copy(out, in)
	return out
}
