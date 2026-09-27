package kwaymerge

import "testing"

func TestMergeTwoStreams(t *testing.T) {
merger := New([][]int{{1, 3, 5}, {2, 4, 6}})
want := []int{1, 2, 3, 4, 5, 6}
got := []int{}
for {
value, ok := merger.Next()
if !ok {
break
}
got = append(got, value)
}
if len(got) != len(want) {
t.Fatalf("条目数不对：%v", got)
}
for index := range want {
if got[index] != want[index] {
t.Fatalf("归并结果不对：%v", got)
}
}
}

func TestEmptyStreams(t *testing.T) {
if _, ok := New([][]int{{}, {}}).Next(); ok {
t.Fatal("空流不该有输出")
}
}

func TestSingleStream(t *testing.T) {
merger := New([][]int{{7, 8}})
if value, _ := merger.Next(); value != 7 {
t.Fatalf("第一个值不对：%d", value)
}
}

func TestHeldCountsItems(t *testing.T) {
if New([][]int{{1, 2, 3}}).Held() != 3 {
t.Fatalf("Held 应该等于条目数：%d", New([][]int{{1, 2, 3}}).Held())
}
}

func TestComparedIsCounted(t *testing.T) {
merger := New([][]int{{1}, {2}})
merger.Next()
if merger.Compared() == 0 {
t.Fatal("比较次数应该被统计")
}
}
