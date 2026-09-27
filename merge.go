// Package kwaymerge 做 k 路有序流归并：大小不超过 k 的堆挑下一个值，
// 任意时刻每路只持有一个待比较的条目，内存 O(k)、Next 为 O(log k)。
package kwaymerge

import "fmt"

// entry 是堆里一个待比较的条目。
type entry struct {
	value  int
	stream int
}

// streamState 记录一路的读取进度与缓冲。
type streamState struct {
	data     []int // 一次性切片模式的数据（属于调用方，只读）
	pos      int   // data 的读取位置
	step     int   // 读取方向：+1 升序从头读，-1 降序（--desc）从尾读
	queue    []int // Push 进来、还没进堆的缓冲
	inHeap   bool  // 堆里当前是否已有这一路的一个条目
	lastPush int
	pushed   bool
}

// Merger 是 k 路归并器。值相同时按路序号升序输出（--desc 时反转），
// 每路内部顺序保持，同一输入重复归并结果逐项一致。
type Merger struct {
	streams []streamState
	items   []entry // 堆，items[0] 是下一个要输出的的条目

	desc   bool
	dedupe bool
	limit  int // 剩余可输出的条数，负数表示不限

	held     int // 持有条目数的峰值（堆内 + 缓冲）
	cur      int // 当前持有的条目数
	compared int // 比较次数

	last    int
	hasLast bool
}

// Option 调整归并行为。
type Option func(*Merger)

// WithDesc 用最大堆输出降序；同值时的稳定规则相应反转（按路序号降序），
// 整体结果是升序结果的逆序。
func WithDesc() Option { return func(m *Merger) { m.desc = true } }

// WithDedupe 让相邻重复值只输出一次。
func WithDedupe() Option { return func(m *Merger) { m.dedupe = true } }

// WithLimit 只输出前 n 个（在去重之后计数）；n < 0 表示不限。
func WithLimit(n int) Option { return func(m *Merger) { m.limit = n } }

// New 用各路的有序切片建一个归并器。切片属于调用方，归并器不为每路
// 拷贝全部数据，任意时刻只缓存每路当前一个待比较的条目。
func New(streams [][]int, opts ...Option) *Merger {
	m := &Merger{streams: make([]streamState, len(streams)), limit: -1}
	for index, data := range streams {
		m.streams[index].data = data
	}
	for _, opt := range opts {
		opt(m)
	}
	for index := range streams {
		m.streams[index].step = 1
		if m.desc {
			// 输入流是升序的，降序归并从每路末尾往前读。
			m.streams[index].step = -1
			m.streams[index].pos = len(streams[index]) - 1
		}
		if next, ok := m.streams[index].next(index); ok {
			m.streams[index].inHeap = true
			m.push(next)
			m.cur++
		}
	}
	m.noteHeld()
	return m
}

// NewStreams 建一个 n 路的增量归并器，配合 Push 与 Pull 边喂边出，
// 不需要先把各路读完再归并。
func NewStreams(n int, opts ...Option) *Merger {
	m := &Merger{streams: make([]streamState, n), limit: -1}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Push 向第 stream 路追加一个值；每路必须按归并方向喂入
// （默认升序，WithDesc 时降序）。
func (m *Merger) Push(stream, value int) {
	if stream < 0 || stream >= len(m.streams) {
		panic(fmt.Sprintf("kwaymerge: 路号 %d 越界（共 %d 路）", stream, len(m.streams)))
	}
	st := &m.streams[stream]
	if st.pushed && ((!m.desc && value < st.lastPush) || (m.desc && value > st.lastPush)) {
		panic(fmt.Sprintf("kwaymerge: 第 %d 路喂入的值 %d 与前一个值 %d 的方向不一致", stream, value, st.lastPush))
	}
	st.pushed, st.lastPush = true, value
	if st.inHeap {
		st.queue = append(st.queue, value)
	} else {
		st.inHeap = true
		m.push(entry{value: value, stream: stream})
	}
	m.cur++
	m.noteHeld()
}

// Pull 取下一个输出值；没有可输出的值时 ok 为 false。
func (m *Merger) Pull() (int, bool) {
	e, ok := m.pullEntry()
	return e.value, ok
}

// Next 是 Pull 的别名。
func (m *Merger) Next() (int, bool) { return m.Pull() }

// PullFrom 取下一个输出值并报告它来自哪一路。
func (m *Merger) PullFrom() (value, stream int, ok bool) {
	e, ok := m.pullEntry()
	return e.value, e.stream, ok
}

// pullEntry 弹出堆顶并立即补进同路的下一个条目（替换堆顶后下沉，
// 每次输出约 2·log k 次比较），再按去重与限流过滤。
func (m *Merger) pullEntry() (entry, bool) {
	for {
		if m.limit == 0 || len(m.items) == 0 {
			return entry{}, false
		}
		top := m.items[0]
		st := &m.streams[top.stream]
		if next, ok := st.next(top.stream); ok {
			m.items[0] = next
		} else {
			last := len(m.items) - 1
			m.items[0] = m.items[last]
			m.items = m.items[:last]
			m.cur--
			st.inHeap = false
		}
		if len(m.items) > 0 {
			m.down(0)
		}
		if m.dedupe {
			if m.hasLast && m.last == top.value {
				continue
			}
			m.last, m.hasLast = top.value, true
		}
		if m.limit > 0 {
			m.limit--
		}
		return top, true
	}
}

// next 取出这一路紧接着的一个条目。
func (s *streamState) next(stream int) (entry, bool) {
	if s.pos >= 0 && s.pos < len(s.data) {
		e := entry{value: s.data[s.pos], stream: stream}
		s.pos += s.step
		return e, true
	}
	if len(s.queue) > 0 {
		e := entry{value: s.queue[0], stream: stream}
		s.queue = s.queue[1:]
		return e, true
	}
	return entry{}, false
}

// Held 是归并器持有条目数的峰值（堆内 + 缓冲），口径要求 <= 2k。
func (m *Merger) Held() int { return m.held }

// Compared 是归并过程中值比较的次数。
func (m *Merger) Compared() int { return m.compared }

func (m *Merger) noteHeld() {
	if m.cur > m.held {
		m.held = m.cur
	}
}

// less 是堆的比较器：升序时小值在前、同值路序号小的在前；降序时两者都反转。
func (m *Merger) less(a, b entry) bool {
	m.compared++
	if a.value != b.value {
		if m.desc {
			return a.value > b.value
		}
		return a.value < b.value
	}
	if m.desc {
		return a.stream > b.stream
	}
	return a.stream < b.stream
}

func (m *Merger) push(e entry) {
	m.items = append(m.items, e)
	for i := len(m.items) - 1; i > 0; {
		parent := (i - 1) / 2
		if !m.less(m.items[i], m.items[parent]) {
			break
		}
		m.items[i], m.items[parent] = m.items[parent], m.items[i]
		i = parent
	}
}

func (m *Merger) down(i int) {
	for {
		child := 2*i + 1
		if child >= len(m.items) {
			return
		}
		if child+1 < len(m.items) && m.less(m.items[child+1], m.items[child]) {
			child++
		}
		if !m.less(m.items[child], m.items[i]) {
			return
		}
		m.items[i], m.items[child] = m.items[child], m.items[i]
		i = child
	}
}
