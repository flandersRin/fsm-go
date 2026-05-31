# 性能测试

workflow-go 的性能目标是可恢复优先。

它不会为了极限吞吐牺牲历史、恢复和可观测能力。

## 生成业务数据

```bash
go run ./scripts/generate-business-data --kind order --count 10000 --out order.jsonl
```

支持类型：

- `order`
- `async_task`
- `saga`
- `agent`
- `failure`

## 内存性能测试

```bash
go run ./scripts/perf --n 20000
```

## Go Benchmark

```bash
go test -run '^$' -bench BenchmarkMemoryRuntimeStartAndRun -benchtime=1x -benchmem ./test/benchmark
```

## 通过标准

当前阶段以内存运行时万级吞吐为目标。数据库运行时以千级吞吐为目标，具体结果受数据库配置和机器性能影响。
