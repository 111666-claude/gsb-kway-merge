package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunDefaultFormat(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Run([]string{"--streams", "4", "--per-stream", "3", "--counters"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("退出码不对：%d（%s）", code, stderr.String())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 2 || lines[0] != "items=12 streams=4" {
		t.Fatalf("输出格式不对：%q", stdout.String())
	}
	if !strings.HasPrefix(lines[1], "held=4 compared=") {
		t.Fatalf("counters 行不对：%q", lines[1])
	}
}

func TestRunWithoutCounters(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := Run([]string{"--streams", "2", "--per-stream", "2"}, &stdout, &stderr); code != 0 {
		t.Fatalf("退出码不对：%d", code)
	}
	if got := strings.TrimSpace(stdout.String()); got != "items=4 streams=2" {
		t.Fatalf("不带新参数时输出格式应与现在一致：%q", got)
	}
}

func TestRunDescShow(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Run([]string{"--streams", "2", "--per-stream", "2", "--desc", "--show"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("退出码不对：%d", code)
	}
	if !strings.HasPrefix(stdout.String(), "result=[3 2 1 0]\n") {
		t.Fatalf("降序结果不对：%q", stdout.String())
	}
}

func TestRunLimit(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Run([]string{"--streams", "4", "--per-stream", "5", "--limit", "3"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("退出码不对：%d", code)
	}
	if !strings.HasPrefix(stdout.String(), "items=3 streams=4\n") {
		t.Fatalf("限流结果不对：%q", stdout.String())
	}
}

func TestRunDedupe(t *testing.T) {
	// 生成的数据每路互不相交，去重后条目数不变，但参数必须可用。
	var stdout, stderr bytes.Buffer
	code := Run([]string{"--streams", "4", "--per-stream", "5", "--dedupe"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("退出码不对：%d", code)
	}
	if !strings.HasPrefix(stdout.String(), "items=20 streams=4\n") {
		t.Fatalf("去重结果不对：%q", stdout.String())
	}
}
