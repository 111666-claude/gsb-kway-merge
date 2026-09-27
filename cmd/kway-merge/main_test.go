package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func run(t *testing.T, args ...string) string {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if code := Run(args, &stdout, &stderr); code != 0 {
		t.Fatalf("退出码 %d，stderr=%s", code, stderr.String())
	}
	return stdout.String()
}

func parseResult(t *testing.T, out string) []int {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if !strings.HasPrefix(line, "result=[") {
			continue
		}
		body := strings.TrimSuffix(strings.TrimPrefix(line, "result=["), "]")
		if body == "" {
			return []int{}
		}
		values := []int{}
		for _, field := range strings.Fields(body) {
			value, err := strconv.Atoi(field)
			if err != nil {
				t.Fatalf("结果里有非数字：%q", field)
			}
			values = append(values, value)
		}
		return values
	}
	t.Fatalf("输出里没有 result 行：%q", out)
	return nil
}

func parseCounters(t *testing.T, out string) (held, compared int) {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "held=") {
			if _, err := fmt.Sscanf(line, "held=%d compared=%d", &held, &compared); err != nil {
				t.Fatalf("counters 行解析失败：%q", line)
			}
			return held, compared
		}
	}
	t.Fatalf("输出里没有 counters 行：%q", out)
	return 0, 0
}

func TestDefaultOutputFormat(t *testing.T) {
	out := run(t, "--streams", "100", "--per-stream", "100", "--counters")
	if !strings.Contains(out, "items=10000 streams=100\n") {
		t.Fatalf("不带新参数时输出格式必须不变：%q", out)
	}
	held, compared := parseCounters(t, out)
	if held > 200 {
		t.Fatalf("held 口径 ≤ 2k：held=%d", held)
	}
	if compared >= 500000 {
		t.Fatalf("compared 应是 N log k 量级（远小于旧的 995050），得到 %d", compared)
	}
}

func TestDescFlagReversesOutput(t *testing.T) {
	asc := parseResult(t, run(t, "--streams", "6", "--per-stream", "7", "--show"))
	desc := parseResult(t, run(t, "--streams", "6", "--per-stream", "7", "--show", "--desc"))
	if len(asc) != len(desc) {
		t.Fatalf("长度不一致：%d vs %d", len(asc), len(desc))
	}
	for i := range asc {
		if desc[i] != asc[len(asc)-1-i] {
			t.Fatalf("--desc 必须是升序的逆序：asc=%v desc=%v", asc, desc)
		}
	}
}

func TestLimitFlag(t *testing.T) {
	out := run(t, "--streams", "4", "--per-stream", "10", "--limit", "7", "--show")
	if got := parseResult(t, out); len(got) != 7 {
		t.Fatalf("--limit 7 应只输出 7 个：%v", got)
	}
	if !strings.Contains(out, "items=7 streams=4\n") {
		t.Fatalf("items 应反映限流后的输出数：%q", out)
	}
}

func TestDedupeFlag(t *testing.T) {
	plain := run(t, "--streams", "4", "--per-stream", "10", "--show")
	deduped := run(t, "--streams", "4", "--per-stream", "10", "--show", "--dedupe")
	// 生成的数据跨路无重复，去重后必须与原输出一致（幂等）。
	if plain != deduped {
		t.Fatalf("无重复数据加 --dedupe 不该改变输出：%q vs %q", plain, deduped)
	}
}

func TestCombinedFlags(t *testing.T) {
	out := run(t, "--streams", "5", "--per-stream", "6", "--show", "--desc", "--limit", "4")
	got := parseResult(t, out)
	if len(got) != 4 {
		t.Fatalf("--desc --limit 4 应输出 4 个：%v", got)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1] < got[i] {
			t.Fatalf("--desc 输出必须降序：%v", got)
		}
	}
	if got[0] != 29 {
		t.Fatalf("降序第一个应是最大值 29：%v", got)
	}
}
