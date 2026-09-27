package kwaymerge

import (
	"math/rand"
	"slices"
	"sort"
	"testing"
)

// reference 是朴素参考实现：全量收集 + 排序，再按序应用去重与限流。
func reference(streams [][]int, desc, dedupe bool, limit int) []int {
	all := []int{}
	for _, stream := range streams {
		all = append(all, stream...)
	}
	sort.Ints(all)
	if desc {
		slices.Reverse(all)
	}
	if dedupe {
		out := all[:0]
		for index, value := range all {
			if index == 0 || value != all[index-1] {
				out = append(out, value)
			}
		}
		all = out
	}
	if limit >= 0 && limit < len(all) {
		all = all[:limit]
	}
	return all
}

func drain(m *Merger) []int {
	out := []int{}
	for {
		value, ok := m.Next()
		if !ok {
			return out
		}
		out = append(out, value)
	}
}

func randomStreams(rng *rand.Rand, k, maxLen, maxValue int) [][]int {
	streams := make([][]int, k)
	for index := range streams {
		length := rng.Intn(maxLen + 1)
		for i := 0; i < length; i++ {
			streams[index] = append(streams[index], rng.Intn(maxValue))
		}
		sort.Ints(streams[index])
	}
	return streams
}

func TestMergeTwoStreams(t *testing.T) {
	merger := New([][]int{{1, 3, 5}, {2, 4, 6}})
	want := []int{1, 2, 3, 4, 5, 6}
	got := drain(merger)
	if !slices.Equal(got, want) {
		t.Fatalf("归并结果不对：got %v want %v", got, want)
	}
}

func TestEmptyStreams(t *testing.T) {
	if _, ok := New([][]int{{}, {}}).Next(); ok {
		t.Fatal("空流不该有输出")
	}
	if _, ok := New(nil).Next(); ok {
		t.Fatal("零路不该有输出")
	}
	if _, ok := NewStreams(0).Pull(); ok {
		t.Fatal("零路不该有输出")
	}
}

func TestSingleStream(t *testing.T) {
	merger := New([][]int{{7, 8}})
	if value, _ := merger.Next(); value != 7 {
		t.Fatalf("第一个值不对：%d", value)
	}
	if got := drain(merger); !slices.Equal(got, []int{8}) {
		t.Fatalf("剩余输出不对：%v", got)
	}
}

func TestStableMergeDuplicateValues(t *testing.T) {
	// 值相同时按路序号升序输出，每路内部顺序保持。
	merger := New([][]int{{1, 2, 2}, {1, 2}, {2}})
	wantValues := []int{1, 1, 2, 2, 2, 2}
	wantStreams := []int{0, 1, 0, 0, 1, 2}
	for index := range wantValues {
		value, stream, ok := merger.PullFrom()
		if !ok || value != wantValues[index] || stream != wantStreams[index] {
			t.Fatalf("第 %d 项不对：got (%d, 第%d路) want (%d, 第%d路)",
				index, value, stream, wantValues[index], wantStreams[index])
		}
	}
}

func TestRepeatedMergeIsDeterministic(t *testing.T) {
	streams := [][]int{{3, 1, 2}, {}, {2, 3}}
	for i := range streams {
		sort.Ints(streams[i])
	}
	first := drain(New(streams))
	second := drain(New(streams))
	if !slices.Equal(first, second) {
		t.Fatalf("同一输入重复归并结果不一致：%v vs %v", first, second)
	}
}

func TestHeldBound(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	streams := randomStreams(rng, 100, 100, 1000)
	merger := New(streams)
	drain(merger)
	if merger.Held() > 2*len(streams) {
		t.Fatalf("held 不许随总条目数增长：held=%d，口径 <= 2k=%d", merger.Held(), 2*len(streams))
	}
	if merger.Held() == 0 {
		t.Fatal("held 应该被统计")
	}
}

func TestComparedMagnitude(t *testing.T) {
	// 与旧实现的 compared=995050（≈ N×k）对照：堆归并应在 N log k 量级。
	rng := rand.New(rand.NewSource(2))
	streams := randomStreams(rng, 100, 100, 100000)
	total := 0
	for _, stream := range streams {
		total += len(stream)
	}
	merger := New(streams)
	drain(merger)
	if merger.Compared() == 0 {
		t.Fatal("比较次数应该被统计")
	}
	// 每次输出约 2·log2(k) 次比较；log2(100) < 7，留足余量取 30·N。
	if bound := 30*total + len(streams)*len(streams); merger.Compared() > bound {
		t.Fatalf("比较次数应是 N log k 量级：compared=%d bound=%d，旧口径 N×k=%d",
			merger.Compared(), bound, total*len(streams))
	}
}

func TestPushPullStreaming(t *testing.T) {
	// 边喂边出：每被消费一路就补喂该路，不需要先把各路读完。
	rng := rand.New(rand.NewSource(3))
	streams := randomStreams(rng, 10, 50, 100)
	pos := make([]int, len(streams))
	feed := func(m *Merger, stream int) {
		if pos[stream] < len(streams[stream]) {
			m.Push(stream, streams[stream][pos[stream]])
			pos[stream]++
		}
	}
	merger := NewStreams(len(streams))
	for stream := range streams {
		feed(merger, stream)
	}
	got := []int{}
	for {
		value, from, ok := merger.PullFrom()
		if !ok {
			break
		}
		got = append(got, value)
		feed(merger, from)
	}
	if want := reference(streams, false, false, -1); !slices.Equal(got, want) {
		t.Fatalf("流式归并与参考实现不一致：got %v want %v", got, want)
	}
	if merger.Held() > 2*len(streams) {
		t.Fatalf("边喂边出时 held 应 <= 2k：held=%d", merger.Held())
	}
}

func TestPushAfterEmpty(t *testing.T) {
	merger := NewStreams(2)
	if _, ok := merger.Pull(); ok {
		t.Fatal("还没喂时不该有输出")
	}
	merger.Push(1, 5)
	merger.Push(0, 3)
	if value, ok := merger.Pull(); !ok || value != 3 {
		t.Fatalf("边喂边出不对：value=%d ok=%v", value, ok)
	}
	merger.Push(0, 7)
	if got := drain(merger); !slices.Equal(got, []int{5, 7}) {
		t.Fatalf("剩余输出不对：%v", got)
	}
}

func TestDedupe(t *testing.T) {
	streams := [][]int{{1, 1, 2}, {1, 3, 3}, {2}}
	got := drain(New(streams, WithDedupe()))
	if want := reference(streams, false, true, -1); !slices.Equal(got, want) {
		t.Fatalf("去重结果与参考实现不一致：got %v want %v", got, want)
	}
	if want := []int{1, 2, 3}; !slices.Equal(got, want) {
		t.Fatalf("去重结果不对：got %v want %v", got, want)
	}
}

func TestLimit(t *testing.T) {
	streams := [][]int{{1, 4}, {2, 5}, {3, 6}}
	got := drain(New(streams, WithLimit(3)))
	if want := []int{1, 2, 3}; !slices.Equal(got, want) {
		t.Fatalf("限流结果不对：got %v want %v", got, want)
	}
	if got := drain(New(streams, WithLimit(0))); len(got) != 0 {
		t.Fatalf("limit=0 不该有输出：%v", got)
	}
}

func TestDedupeThenLimit(t *testing.T) {
	// 与参考实现一致：先去重，再取前 N 个。
	streams := [][]int{{1, 1, 1, 2}, {1, 2, 2, 3}}
	got := drain(New(streams, WithDedupe(), WithLimit(2)))
	if want := reference(streams, false, true, 2); !slices.Equal(got, want) {
		t.Fatalf("去重+限流与参考实现不一致：got %v want %v", got, want)
	}
	if want := []int{1, 2}; !slices.Equal(got, want) {
		t.Fatalf("去重+限流结果不对：got %v want %v", got, want)
	}
}

func TestDesc(t *testing.T) {
	streams := [][]int{{1, 3, 5}, {2, 4, 6}}
	got := drain(New(streams, WithDesc()))
	if want := []int{6, 5, 4, 3, 2, 1}; !slices.Equal(got, want) {
		t.Fatalf("降序结果不对：got %v want %v", got, want)
	}
}

func TestDescIsReverseOfAsc(t *testing.T) {
	// 降序结果必须是升序结果的逆序，同值时的稳定规则也相应反转。
	rng := rand.New(rand.NewSource(4))
	for round := 0; round < 100; round++ {
		streams := randomStreams(rng, 1+rng.Intn(8), 20, 10)
		asc := drain(New(streams))
		desc := drain(New(streams, WithDesc()))
		slices.Reverse(asc)
		if !slices.Equal(asc, desc) {
			t.Fatalf("降序不是升序的逆序：asc 逆序 %v，desc %v", asc, desc)
		}
	}
}

func TestDescStability(t *testing.T) {
	// 降序下同值按路序号降序输出（稳定规则反转）。
	merger := New([][]int{{1, 2, 2}, {1, 2}, {2}}, WithDesc())
	wantValues := []int{2, 2, 2, 2, 1, 1}
	wantStreams := []int{2, 1, 0, 0, 1, 0}
	for index := range wantValues {
		value, stream, ok := merger.PullFrom()
		if !ok || value != wantValues[index] || stream != wantStreams[index] {
			t.Fatalf("第 %d 项不对：got (%d, 第%d路) want (%d, 第%d路)",
				index, value, stream, wantValues[index], wantStreams[index])
		}
	}
}

func TestRandomAgainstReference(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	for round := 0; round < 1000; round++ {
		k := 1 + rng.Intn(8)
		streams := randomStreams(rng, k, 20, 30)
		desc := rng.Intn(2) == 0
		dedupe := rng.Intn(2) == 0
		limit := -1
		if rng.Intn(2) == 0 {
			limit = rng.Intn(40)
		}
		opts := []Option{}
		if desc {
			opts = append(opts, WithDesc())
		}
		if dedupe {
			opts = append(opts, WithDedupe())
		}
		if limit >= 0 {
			opts = append(opts, WithLimit(limit))
		}
		got := drain(New(streams, opts...))
		want := reference(streams, desc, dedupe, limit)
		if !slices.Equal(got, want) {
			t.Fatalf("第 %d 组与参考实现不一致（desc=%v dedupe=%v limit=%d）：\ngot  %v\nwant %v",
				round, desc, dedupe, limit, got, want)
		}
	}
}

func TestScaleMillionItems(t *testing.T) {
	// 100 万条目、k=1000：内存 O(k)，时间 O(N log k)。
	const k = 1000
	const perStream = 1000
	const total = k * perStream
	rng := rand.New(rand.NewSource(6))
	streams := make([][]int, k)
	for index := range streams {
		for i := 0; i < perStream; i++ {
			streams[index] = append(streams[index], rng.Intn(1<<30))
		}
		sort.Ints(streams[index])
	}
	actual := k * perStream
	merger := New(streams)
	count := 0
	prev := 0
	for {
		value, ok := merger.Next()
		if !ok {
			break
		}
		if count > 0 && value < prev {
			t.Fatalf("输出不是升序：第 %d 项 %d 前一项 %d", count, value, prev)
		}
		prev = value
		count++
	}
	if count != actual {
		t.Fatalf("输出条目数不等于各路长度之和：%d != %d", count, actual)
	}
	if merger.Held() > 2*k {
		t.Fatalf("100 万条目时 held 仍须 <= 2k：held=%d", merger.Held())
	}
	// 每次输出约 2·log2(k) 次比较，留足余量；旧实现会是 N×k ≈ 10^9 量级。
	if bound := 4*total*10 + k*k; merger.Compared() > bound {
		t.Fatalf("比较次数超出 N log k 量级：compared=%d bound=%d", merger.Compared(), bound)
	}
	t.Logf("total=%d held=%d compared=%d", actual, merger.Held(), merger.Compared())
}

func TestPushPullDesc(t *testing.T) {
	// 降序增量归并：每路按降序喂入，结果仍是升序结果的逆序。
	asc := [][]int{{1, 3, 5}, {2, 4, 6}}
	merger := NewStreams(2, WithDesc())
	for stream := range asc {
		for index := len(asc[stream]) - 1; index >= 0; index-- {
			merger.Push(stream, asc[stream][index])
		}
	}
	if got, want := drain(merger), []int{6, 5, 4, 3, 2, 1}; !slices.Equal(got, want) {
		t.Fatalf("降序 Push/Pull 不对：got %v want %v", got, want)
	}
}
