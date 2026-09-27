package kwaymerge

import (
	"math/bits"
	"math/rand"
	"sort"
	"testing"
)

// referenceMerge 是朴素参考实现：全量收集（held = 总条目数 N）+ 稳定排序。
// 逐路追加后只按值做稳定排序，同值自然按路序号升序、路内保持原顺序；
// desc 按口径定义为升序结果的逆序。
func referenceMerge(streams [][]int, desc bool) (values, sources []int) {
	type pair struct{ value, stream int }
	all := []pair{}
	for stream, data := range streams {
		for _, value := range data {
			all = append(all, pair{value, stream})
		}
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].value < all[j].value })
	for _, p := range all {
		values = append(values, p.value)
		sources = append(sources, p.stream)
	}
	if desc {
		for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
			values[i], values[j] = values[j], values[i]
			sources[i], sources[j] = sources[j], sources[i]
		}
	}
	return values, sources
}

func referenceDrain(values []int, dedupe bool, limit int) []int {
	out := []int{}
	prev := 0
	havePrev := false
	for _, value := range values {
		if limit >= 0 && len(out) >= limit {
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

func drainAll(m *Merger) (values, sources []int) {
	for {
		value, stream, ok := m.NextWithSource()
		if !ok {
			return values, sources
		}
		values = append(values, value)
		sources = append(sources, stream)
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// comparedBound 是 O(N log k) 的上界：每次输出一次 Pop（≤2log k 次比较）
// 加一次 refill Push（≤log k 次比较）。
func comparedBound(n, k int) int {
	if k < 1 {
		k = 1
	}
	return 3*n*(bits.Len(uint(k))) + 2*k
}

func randomStreams(rng *rand.Rand, maxK, maxLen, maxValue int) [][]int {
	k := 1 + rng.Intn(maxK)
	streams := make([][]int, k)
	for i := range streams {
		n := rng.Intn(maxLen + 1)
		value := rng.Intn(maxValue)
		for j := 0; j < n; j++ {
			value += rng.Intn(4)
			streams[i] = append(streams[i], value)
		}
	}
	return streams
}

func TestMergeTwoStreams(t *testing.T) {
	merger := New([][]int{{1, 3, 5}, {2, 4, 6}})
	want := []int{1, 2, 3, 4, 5, 6}
	got, _ := drainAll(merger)
	if !equalInts(got, want) {
		t.Fatalf("归并结果不对：got=%v want=%v", got, want)
	}
}

func TestEmptyStreams(t *testing.T) {
	if _, ok := New([][]int{{}, {}}).Next(); ok {
		t.Fatal("空流不该有输出")
	}
	if _, ok := New(nil).Next(); ok {
		t.Fatal("零路不该有输出")
	}
	if _, ok := NewStreaming(3).Pull(); ok {
		t.Fatal("没喂数据的流式归并器不该有输出")
	}
}

func TestSingleStream(t *testing.T) {
	merger := New([][]int{{7, 8}})
	if value, _ := merger.Next(); value != 7 {
		t.Fatalf("第一个值不对：%d", value)
	}
	if value, _ := merger.Next(); value != 8 {
		t.Fatalf("第二个值不对：%d", value)
	}
	if _, ok := merger.Next(); ok {
		t.Fatal("耗尽后不该有输出")
	}
}

func TestStableWithDuplicateValues(t *testing.T) {
	streams := [][]int{{1, 5, 5}, {5}, {5, 9}}
	values, sources := drainAll(New(streams))
	if !equalInts(values, []int{1, 5, 5, 5, 5, 9}) {
		t.Fatalf("值序列不对：%v", values)
	}
	if !equalInts(sources, []int{0, 0, 0, 1, 2, 2}) {
		t.Fatalf("同值必须按路序号升序、路内保持顺序：%v", sources)
	}
}

func TestRepeatable(t *testing.T) {
	streams := [][]int{{3, 1, 1}, {1, 2}, {}, {1, 3}}
	for i := range streams {
		sort.Ints(streams[i])
	}
	first, firstSrc := drainAll(New(streams))
	second, secondSrc := drainAll(New(streams))
	if !equalInts(first, second) || !equalInts(firstSrc, secondSrc) {
		t.Fatalf("同一输入重复归并必须一致：%v/%v vs %v/%v", first, firstSrc, second, secondSrc)
	}
}

func TestDescIsExactReverse(t *testing.T) {
	streams := [][]int{{1, 5, 5}, {5}, {5, 9}, {}}
	ascValues, ascSources := drainAll(New(streams))
	descValues, descSources := drainAll(New(streams, Desc()))
	n := len(ascValues)
	for i := 0; i < n; i++ {
		if descValues[i] != ascValues[n-1-i] || descSources[i] != ascSources[n-1-i] {
			t.Fatalf("降序必须是升序的逆序：asc=%v/%v desc=%v/%v",
				ascValues, ascSources, descValues, descSources)
		}
	}
}

func TestPushPullStreaming(t *testing.T) {
	streams := [][]int{{1, 4, 4, 9}, {2, 4}, {}, {0, 10}}
	want, wantSrc := referenceMerge(streams, false)
	merger := NewStreaming(len(streams))
	next := make([]int, len(streams))
	inflight := make([]bool, len(streams))
	got, gotSrc := []int{}, []int{}
	for len(got) < len(want) {
		for s := range streams { // 边喂：给空槽的路补一个待消费值
			if !inflight[s] && next[s] < len(streams[s]) {
				if err := merger.Push(s, streams[s][next[s]]); err != nil {
					t.Fatalf("Push 失败：%v", err)
				}
				next[s]++
				inflight[s] = true
			}
		}
		value, stream, ok := merger.NextWithSource() // 边出
		if !ok {
			t.Fatal("还没喂完就没输出了")
		}
		inflight[stream] = false
		got = append(got, value)
		gotSrc = append(gotSrc, stream)
	}
	if !equalInts(got, want) || !equalInts(gotSrc, wantSrc) {
		t.Fatalf("流式结果与参考不一致：got=%v/%v want=%v/%v", got, gotSrc, want, wantSrc)
	}
	if merger.Held() > 2*len(streams) {
		t.Fatalf("流式 held 必须 ≤ 2k：held=%d k=%d", merger.Held(), len(streams))
	}
}

func TestPushPullStreamingDesc(t *testing.T) {
	streams := [][]int{{1, 4, 4, 9}, {2, 4}, {0, 10}}
	want, _ := referenceMerge(streams, true)
	merger := NewStreaming(len(streams), Desc())
	next := make([]int, len(streams))
	inflight := make([]bool, len(streams))
	got := []int{}
	for len(got) < len(want) {
		for s := range streams { // desc 模式每路按降序喂
			if !inflight[s] && next[s] < len(streams[s]) {
				value := streams[s][len(streams[s])-1-next[s]]
				if err := merger.Push(s, value); err != nil {
					t.Fatalf("Push 失败：%v", err)
				}
				next[s]++
				inflight[s] = true
			}
		}
		value, stream, ok := merger.NextWithSource()
		if !ok {
			t.Fatal("还没喂完就没输出了")
		}
		inflight[stream] = false
		got = append(got, value)
	}
	if !equalInts(got, want) {
		t.Fatalf("流式降序与参考不一致：got=%v want=%v", got, want)
	}
}

func TestPushErrors(t *testing.T) {
	merger := NewStreaming(2)
	if err := merger.Push(0, 1); err != nil {
		t.Fatalf("正常 Push 不该报错：%v", err)
	}
	if err := merger.Push(0, 2); err == nil {
		t.Fatal("同一路有未消费值时 Push 必须报错")
	}
	if err := merger.Push(2, 1); err == nil {
		t.Fatal("路号越界必须报错")
	}
	if err := merger.Push(-1, 1); err == nil {
		t.Fatal("负路号必须报错")
	}
	if err := New([][]int{{1}}).Push(0, 1); err == nil {
		t.Fatal("切片模式的归并器不该接受 Push")
	}
}

func TestDrainDedupeAndLimit(t *testing.T) {
	streams := [][]int{{1, 1, 2, 2, 2, 5}, {1, 2, 3}, {3, 3}}
	all, _ := referenceMerge(streams, false)
	cases := []struct {
		dedupe bool
		limit  int
	}{
		{false, -1},
		{true, -1},
		{false, 0},
		{false, 4},
		{true, 3},
		{true, 100},
	}
	for _, tc := range cases {
		got := New(streams).Drain(tc.dedupe, tc.limit)
		want := referenceDrain(all, tc.dedupe, tc.limit)
		if !equalInts(got, want) {
			t.Fatalf("Drain(dedupe=%v, limit=%d) = %v，参考 = %v", tc.dedupe, tc.limit, got, want)
		}
	}
	descAll, _ := referenceMerge(streams, true)
	got := New(streams, Desc()).Drain(true, 4)
	if want := referenceDrain(descAll, true, 4); !equalInts(got, want) {
		t.Fatalf("desc+dedupe+limit = %v，参考 = %v", got, want)
	}
}

func TestHeldWithinBound(t *testing.T) {
	const k = 100
	const perStream = 100
	streams := make([][]int, k)
	for s := range streams {
		for v := s; v < k*perStream; v += k {
			streams[s] = append(streams[s], v)
		}
	}
	merger := New(streams)
	merger.Drain(false, -1)
	if merger.Held() > 2*k {
		t.Fatalf("held 必须 ≤ 2k：held=%d k=%d", merger.Held(), k)
	}
	if merger.Held() != k {
		t.Fatalf("每路只留一个头，峰值应等于非空路数 k：held=%d", merger.Held())
	}
}

func TestComparedIsCounted(t *testing.T) {
	merger := New([][]int{{1}, {2}})
	merger.Next()
	if merger.Compared() == 0 {
		t.Fatal("比较次数应该被统计")
	}
}

func TestComparedIsNLogK(t *testing.T) {
	const k = 100
	const perStream = 100
	const n = k * perStream
	streams := make([][]int, k)
	for s := range streams {
		for v := s; v < n; v += k {
			streams[s] = append(streams[s], v)
		}
	}
	merger := New(streams)
	merger.Drain(false, -1)
	if merger.Compared() > comparedBound(n, k) {
		t.Fatalf("compared 超出 O(N log k) 上界：%d > %d", merger.Compared(), comparedBound(n, k))
	}
	if merger.Compared()*4 >= n*k {
		t.Fatalf("compared 必须远小于旧实现的 N×k 量级：%d vs N×k=%d", merger.Compared(), n*k)
	}
}

func TestRandomAgainstReference(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for trial := 0; trial < 1000; trial++ {
		streams := randomStreams(rng, 8, 12, 20)
		k := len(streams)

		wantValues, wantSources := referenceMerge(streams, false)
		gotValues, gotSources := drainAll(New(streams))
		if !equalInts(gotValues, wantValues) || !equalInts(gotSources, wantSources) {
			t.Fatalf("trial %d 升序与参考不一致：got=%v/%v want=%v/%v",
				trial, gotValues, gotSources, wantValues, wantSources)
		}

		wantDescValues, wantDescSources := referenceMerge(streams, true)
		gotDescValues, gotDescSources := drainAll(New(streams, Desc()))
		if !equalInts(gotDescValues, wantDescValues) || !equalInts(gotDescSources, wantDescSources) {
			t.Fatalf("trial %d 降序不是升序的逆序", trial)
		}

		dedupe := trial%2 == 0
		limit := -1
		if trial%3 == 0 {
			limit = rng.Intn(len(wantValues) + 2)
		}
		if got, want := New(streams).Drain(dedupe, limit),
			referenceDrain(wantValues, dedupe, limit); !equalInts(got, want) {
			t.Fatalf("trial %d Drain(%v,%d) 与参考不一致：got=%v want=%v", trial, dedupe, limit, got, want)
		}

		merger := New(streams)
		merger.Drain(false, -1)
		if merger.Held() > 2*k {
			t.Fatalf("trial %d held 越界：%d > 2k=%d", trial, merger.Held(), 2*k)
		}
		if merger.Compared() > comparedBound(len(wantValues), k) {
			t.Fatalf("trial %d compared 越界：%d", trial, merger.Compared())
		}
	}
}

func TestScaleMillionEntries(t *testing.T) {
	const k = 1000
	const perStream = 1000
	const n = k * perStream // 100 万条目
	rng := rand.New(rand.NewSource(7))
	streams := make([][]int, k)
	for s := range streams {
		value := rng.Intn(1000)
		for j := 0; j < perStream; j++ {
			value += rng.Intn(10)
			streams[s] = append(streams[s], value)
		}
	}

	merger := New(streams)
	got, _ := drainAll(merger)
	want, _ := referenceMerge(streams, false) // 参考实现 held = N = 1e6
	if !equalInts(got, want) {
		t.Fatal("100 万条目的归并结果与参考不一致")
	}
	if merger.Held() > 2*k {
		t.Fatalf("内存口径 O(k)：held=%d 应 ≤ 2k=%d（参考实现 held=%d）", merger.Held(), 2*k, n)
	}
	if merger.Compared() > comparedBound(n, k) {
		t.Fatalf("时间口径 O(N log k)：compared=%d 应 ≤ %d", merger.Compared(), comparedBound(n, k))
	}
	t.Logf("k=%d N=%d：held=%d（参考 %d），compared=%d（上界 %d，旧线性扫头约 %d）",
		k, n, merger.Held(), n, merger.Compared(), comparedBound(n, k), n*k)
}
