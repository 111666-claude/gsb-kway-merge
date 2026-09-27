# kway-merge

把 k 路有序流归并成一路，只用 Go 标准库。

```
go test ./...
go run ./cmd/kway-merge --streams 100 --per-stream 100 --counters
```

## 口径（README 为准）

- **用大小为 k 的小顶堆**挑下一个最小值：`Next()` 是 O(log k)，整体 O(N log k)。
  `--streams 100 --per-stream 100 --counters` 现在 `compared≈114508`（N log k 量级，
  约 7 万的常数倍），修复前是 `compared=995050`（≈ N×k）。
- **内存 O(k)**：只保留每路当前那一个待比较的条目，不把各路全部读进内存。
  现在 `held=100`（= k，口径 ≤ 2k），修复前 `held=10000`（等于总条目数）。
- 归并结果与「全部拼起来排序」完全一致（顺序与元素都要一致），空流不产生输出。
- 不变量：输出条目数等于各路长度之和；每路内部顺序被保留（稳定）；同一输入重复归并结果相同。
- 规模：100 万条目、k=1000 时内存 O(k)，时间 O(N log k)；不许每次输出都重扫全部 k 个头。

## 实现方式

- 堆顶是下一个要输出的条目；弹出后立刻补进同路的下一个条目并下沉，
  每次输出约 2·log k 次比较。任意时刻堆里每路至多一个条目。
- 稳定归并：值相同时按路序号升序输出；`--desc` 时稳定规则相应反转，
  降序结果是升序结果的逆序。
- 流式接口：除 `New(各路切片)` 外，`NewStreams(n)` + `Push(路号, 值)` + `Pull()`
  支持边喂边出，不需要先把各路读完（每路须按归并方向喂入）。

## 参数

```
--streams N --per-stream M    生成 N 路、每路 M 个有序条目
--counters                    打印 held 与 compared
--show                        打印归并结果
--dedupe                      相邻重复值只输出一次
--limit N                     只输出前 N 个（在去重之后计数）
--desc                        用最大堆输出降序（升序结果的逆序）
```

## 输出

```
items=… streams=…
held=… compared=…
```

## 目录

```
merge.go           归并器（堆、Push/Pull、去重/限流/降序）
cmd/kway-merge     命令行入口
merge_test.go      go test 用例（含朴素参考实现对照）
```
