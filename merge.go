// Package kwaymerge 做 k 路有序流归并。
package kwaymerge

// Merger 归并多个有序流。缺陷：先把每路全部读进内存，而且每次输出都重扫 k 个头。
type Merger struct {
	streams  [][]int
	heads    []int
	held     int
	compared int
}

// New 建一个归并器，streams 是各路的完整数据（口径要求流式，见 README）。
func New(streams [][]int) *Merger {
	copied := make([][]int, len(streams))
	total := 0
	for index, stream := range streams {
		copied[index] = append([]int{}, stream...)
		total += len(stream)
	}
	return &Merger{streams: copied, heads: make([]int, len(streams)), held: total}
}

// Next 取下一个最小值。缺陷：线性扫描所有头找最小，O(k) 每次；没有堆。
func (m *Merger) Next() (int, bool) {
	best := -1
	for index := range m.streams {
		if m.heads[index] >= len(m.streams[index]) {
			continue
		}
		m.compared++
		if best < 0 || m.streams[index][m.heads[index]] < m.streams[best][m.heads[best]] {
			best = index
		}
	}
	if best < 0 {
		return 0, false
	}
	value := m.streams[best][m.heads[best]]
	m.heads[best]++
	return value, true
}

// Held 是当前占用的条目数（统计用）。
func (m *Merger) Held() int { return m.held }

// Compared 是比较次数（统计用）。
func (m *Merger) Compared() int { return m.compared }
