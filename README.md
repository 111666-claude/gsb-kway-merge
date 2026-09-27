# kway-merge

把 k 路有序流归并成一路，只用 Go 标准库（`container/heap`）。

```
go test ./...
go run ./cmd/kway-merge --streams 100 --per-stream 100 --counters
```

## 口径（README 为准）

- **用大小为 k 的小顶堆**挑下一个最小值：`Next()` 是 O(log k)，整体 O(N log k)。
  `--streams 100 --per-stream 100 --counters` 现在 `held=100 compared=124240`，
  符合 N log k 量级（旧实现是 N×k ≈ 995050）。
- **内存 O(k)**：只保留每路当前那一个待比较的条目，`held`（峰值）≤ k，口径 ≤ 2k。
  旧实现把每路全部拷进内存，`held` 等于总条目数（10000）。
- 归并结果与「全部拼起来稳定排序」完全一致（顺序与元素都要一致），空流不产生输出。
- 不变量：输出条目数等于各路长度之和；同值按路序号升序、每路内部顺序保留（稳定）；
  同一输入重复归并结果逐项一致。
- 规模：100 万条目、k=1000 时 `held=1000`（参考实现为 1e6），
  `compared≈1.9e7`（旧线性扫头约 1e9），内存 O(k)、时间 O(N log k)。

## 实现

- `New(streams)` 不再拷贝各路数据，堆里只放每路当前头一个条目。
- `NewStreaming(k)` + `Push(路号, 值)` / `Pull()`：流式增量接口，边喂边出，
  每路同一时间最多一个未消费值，不需要先把任何一路读完。
- `NextWithSource()` 额外返回值来自哪一路，用于校验稳定性。
- `Drain(dedupe, limit)`：相邻重复值只输出一次、最多输出前 N 个。
- `Desc()` 选项：最大堆降序输出，同值时路序号大的先出，
  结果正好是升序结果的逆序（切片模式从每路尾部消费；流式模式每路按降序 Push）。

## 参数

```
--streams N --per-stream M    生成 N 路、每路 M 个有序条目
--counters                    打印 held 与 compared
--show                        打印归并结果
--dedupe                      相邻重复值只输出一次
--limit N                     只输出前 N 个（负数表示不限）
--desc                        降序输出（升序结果的逆序）
```

## 输出

```
items=… streams=…
held=… compared=…
```

## 目录

```
merge.go                     归并器（堆 + 流式接口）
cmd/kway-merge               命令行入口
merge_test.go                go test 用例（含朴素参考实现对照）
cmd/kway-merge/main_test.go  命令行参数用例
```
