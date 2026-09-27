// Command kway-merge 归并若干有序流并打印结果统计。
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"example.com/kwaymerge"
)

// Run 执行一次命令行调用，返回退出码。
func Run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("kway-merge", flag.ContinueOnError)
	flags.SetOutput(stderr)
	streams := flags.Int("streams", 4, "路的数量")
	perStream := flags.Int("per-stream", 25, "每路的条目数")
	counters := flags.Bool("counters", false, "打印 held 与 compared")
	show := flags.Bool("show", false, "打印归并结果")
	dedupe := flags.Bool("dedupe", false, "相邻重复值只输出一次")
	limit := flags.Int("limit", -1, "只输出前 N 个（负数表示不限）")
	desc := flags.Bool("desc", false, "用最大堆输出降序")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	data := make([][]int, *streams)
	for s := 0; s < *streams; s++ {
		for value := s; value < *streams*(*perStream); value += *streams {
			data[s] = append(data[s], value)
		}
	}
	opts := []kwaymerge.Option{}
	if *dedupe {
		opts = append(opts, kwaymerge.WithDedupe())
	}
	if *limit >= 0 {
		opts = append(opts, kwaymerge.WithLimit(*limit))
	}
	if *desc {
		opts = append(opts, kwaymerge.WithDesc())
	}
	merger := kwaymerge.New(data, opts...)
	out := []int{}
	for {
		value, ok := merger.Next()
		if !ok {
			break
		}
		out = append(out, value)
	}
	if *show {
		fmt.Fprintf(stdout, "result=%v\n", out)
	}
	fmt.Fprintf(stdout, "items=%d streams=%d\n", len(out), *streams)
	if *counters {
		fmt.Fprintf(stdout, "held=%d compared=%d\n", merger.Held(), merger.Compared())
	}
	return 0
}

func main() {
	os.Exit(Run(os.Args[1:], os.Stdout, os.Stderr))
}
