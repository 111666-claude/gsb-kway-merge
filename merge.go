// Package kwaymerge 做 k 路有序流归并：大小为 k 的堆 + O(k) 内存。
package kwaymerge

import (
	"container/heap"
	"errors"
	"fmt"
)

// Option 配置归并器行为。
type Option func(*config)

type config struct {
	desc bool
}

// Desc 让归并器按降序输出（内部用最大堆）。切片模式下每路仍按升序提供，
// 归并器从每路尾部开始消费；流式模式下每一路必须按降序 Push。
// 同值时路序号大的先输出，因此降序结果正好是升序结果的逆序。
func Desc() Option {
	return func(cfg *config) { cfg.desc = true }
}

type entry struct {
	value  int
	stream int
}

// entryHeap 按 (值, 路序号) 排序的堆；desc 时两个维度都取反。
type entryHeap struct {
	items    []entry
	desc     bool
	compared *int
}

func (h *entryHeap) Len() int { return len(h.items) }

func (h *entryHeap) Less(i, j int) bool {
	(*h.compared)++
	a, b := h.items[i], h.items[j]
	if a.value != b.value {
		if h.desc {
			return a.value > b.value
		}
		return a.value < b.value
	}
	if h.desc {
		return a.stream > b.stream
	}
	return a.stream < b.stream
}

func (h *entryHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *entryHeap) Push(item any) { h.items = append(h.items, item.(entry)) }

func (h *entryHeap) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	old[n-1] = entry{}
	h.items = old[:n-1]
	return item
}

// Merger 把 k 路有序流归并成一路。任意时刻每路最多只有一个待比较条目在堆里，
// 内存占用 O(k)，Next/Pull 是 O(log k)，整体 O(N log k)。
type Merger struct {
	heap      entryHeap
	slices    [][]int // 切片模式下的输入（不拷贝）
	pos       []int   // 切片模式下每路下一个待消费的位置
	streaming bool
	pending   []bool // 流式模式下每路是否已有未消费条目
	held      int    // 归并过程中同时持有条目数的峰值
	compared  int    // 堆比较次数
}

// New 用各路完整切片建归并器。切片不会被拷贝，归并器只保留每路当前头条目。
func New(streams [][]int, opts ...Option) *Merger {
	m := newMerger(opts)
	m.slices = streams
	m.pos = make([]int, len(streams))
	for index := range streams {
		if m.heap.desc {
			m.pos[index] = len(streams[index]) - 1
		}
		m.refill(index)
	}
	return m
}

// NewStreaming 建一个 numStreams 路的流式归并器，配合 Push/Pull 边喂边出，
// 不需要先把任何一路读完。
func NewStreaming(numStreams int, opts ...Option) *Merger {
	if numStreams < 0 {
		numStreams = 0
	}
	m := newMerger(opts)
	m.streaming = true
	m.pending = make([]bool, numStreams)
	return m
}

func newMerger(opts []Option) *Merger {
	cfg := config{}
	for _, opt := range opts {
		opt(&cfg)
	}
	m := &Merger{}
	m.heap.desc = cfg.desc
	m.heap.compared = &m.compared
	return m
}

// refill 把一路的下一个条目放进堆里（仅切片模式）。
func (m *Merger) refill(stream int) {
	if m.heap.desc {
		if m.pos[stream] < 0 {
			return
		}
	} else if m.pos[stream] >= len(m.slices[stream]) {
		return
	}
	heap.Push(&m.heap, entry{value: m.slices[stream][m.pos[stream]], stream: stream})
	if m.heap.desc {
		m.pos[stream]--
	} else {
		m.pos[stream]++
	}
	m.trackHeld()
}

func (m *Merger) trackHeld() {
	if m.heap.Len() > m.held {
		m.held = m.heap.Len()
	}
}

// Push 给一路喂下一个值。每一路同一时间最多一个未消费的值；
// 升序模式每路按升序喂，Desc 模式每路按降序喂。
func (m *Merger) Push(stream, value int) error {
	if !m.streaming {
		return errors.New("kwaymerge: Push 只用于 NewStreaming 建的归并器")
	}
	if stream < 0 || stream >= len(m.pending) {
		return fmt.Errorf("kwaymerge: 路号 %d 超出范围 [0,%d)", stream, len(m.pending))
	}
	if m.pending[stream] {
		return fmt.Errorf("kwaymerge: 路 %d 还有未消费的值，先 Pull 再 Push", stream)
	}
	m.pending[stream] = true
	heap.Push(&m.heap, entry{value: value, stream: stream})
	m.trackHeld()
	return nil
}

func (m *Merger) pop() (entry, bool) {
	if m.heap.Len() == 0 {
		return entry{}, false
	}
	item := heap.Pop(&m.heap).(entry)
	if m.streaming {
		m.pending[item.stream] = false
	} else {
		m.refill(item.stream)
	}
	return item, true
}

// Next 返回下一个值；ok 为 false 表示所有路都耗尽。
func (m *Merger) Next() (value int, ok bool) {
	value, _, ok = m.NextWithSource()
	return value, ok
}

// Pull 是流式接口的读取端，语义与 Next 相同。
func (m *Merger) Pull() (int, bool) { return m.Next() }

// NextWithSource 同 Next，额外返回该值来自哪一路，便于校验稳定性。
func (m *Merger) NextWithSource() (value, stream int, ok bool) {
	item, ok := m.pop()
	if !ok {
		return 0, 0, false
	}
	return item.value, item.stream, true
}

// Drain 读出剩余的全部值；dedupe 为真时相邻重复值只保留一个，
// limit >= 0 时最多输出 limit 个。
func (m *Merger) Drain(dedupe bool, limit int) []int {
	out := []int{}
	prev := 0
	havePrev := false
	for limit < 0 || len(out) < limit {
		value, ok := m.Next()
		if !ok {
			break
		}
		if dedupe && havePrev && value == prev {
			continue
		}
		out = append(out, value)
		prev = value
		havePrev = true
	}
	return out
}

// Held 是归并过程中同时持有条目数的峰值，口径要求 ≤ 2k。
func (m *Merger) Held() int { return m.held }

// Compared 是堆比较次数，量级为 O(N log k)。
func (m *Merger) Compared() int { return m.compared }
