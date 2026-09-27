# kway-merge

把 k 路有序流归并成一路，只用 Go 标准库。

```
go test ./...
go run ./cmd/kway-merge --streams 100 --per-stream 100 --counters
```

## 口径（README 为准）

- **用大小为 k 的小顶堆**挑下一个最小值：`Next()` 是 O(log k)，整体 O(N log k)。
  `--streams 100 --per-stream 100 --counters` 现在 `compared=995050`（≈ N×k），
  按口径应该在同一量级（N log k，约 7 万）。
- **内存 O(k)**：只保留每路当前那一个待比较的条目，不许把各路全部读进内存。
  现在 `held=10000`（等于总条目数），按口径应该 ≤ 2k。
- 归并结果必须与「全部拼起来排序」完全一致（顺序与元素都要一致），空流不产生输出。
- 不变量：输出条目数等于各路长度之和；每路内部顺序被保留（稳定）；同一输入重复归并结果相同。
- 规模：100 万条目、k=1000 时内存 O(k)，时间 O(N log k)；不许每次输出都重扫全部 k 个头。

## 现在的行为

- `New` 把每一路都 `append([]int{}, stream...)` 拷进内存：`held` 等于总条目数（10000）。
- `Next` 线性扫描 k 个头找最小：比较次数 ≈ N×k（`compared=995050`）。
- 没有堆结构，`container/heap` 也没用上；k 越大越慢。

## 计划新增的能力

```
--streams N --per-stream M    生成 N 路、每路 M 个有序条目
--counters                    打印 held 与 compared
```

## 输出

```
items=… streams=…
held=… compared=…
```

## 目录

```
merge.go           归并器
cmd/kway-merge     命令行入口
merge_test.go      go test 用例
```
