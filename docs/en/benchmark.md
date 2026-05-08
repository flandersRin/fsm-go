# Benchmark

FSM Go includes a 100K transition benchmark for measuring runtime latency, throughput, and memory allocation under batch workloads.

The benchmark covers two scenarios:

- `without_observability`: core runtime transitions only.
- `with_prometheus_observability`: the same workload with Prometheus observability enabled.

## Command

```bash
go test -run '^$' -bench BenchmarkRuntimeFire100K -benchtime=1x -benchmem ./test/benchmark
```

With Taskfile:

```bash
task test:benchmark
```

Without installing Taskfile globally:

```bash
go run github.com/go-task/task/v3/cmd/task@v3.50.0 test:benchmark
```

## What It Measures

Each benchmark run preloads 100,000 state entities, then fires one transition for each entity. Timing and allocation metrics cover only the transition phase, not preload data or command construction.

Every transition goes through:

- Compiled transition rule matching.
- Guard expression evaluation.
- CAS state update.
- State log write.
- Idempotency result save.

With observability enabled, it also records:

- Transition count.
- Transition duration.
- Error count.
- Idempotency hit count.
- In-flight transition count.

## Local Sample Result

The following result was measured on an Apple M1 Pro. It is a local sample only. Actual results depend on hardware, Go version, and runtime load.

| Scenario | Total Time | Per Transition | Throughput | Allocation |
|---|---:|---:|---:|---:|
| Without observability | 109.5 ms | 1,095 ns | 912,859 transitions/s | 83.92 MB |
| With Prometheus observability | 157.4 ms | 1,574 ns | 635,508 transitions/s | 83.93 MB |

In this run, Prometheus observability increased per-transition latency by about 43.7%. The overhead mainly comes from metric counting, label writes, and duration recording. Even with observability enabled, 100,000 transitions complete within 0.2 seconds.

## Allocation Sources

The earlier result was close to 700 MB mainly because:

- Guard expressions were parsed and compiled on every transition.
- Benchmark allocation stats included preloading 100,000 entities and building commands.
- The in-memory test Repository used concatenated string keys, and logs plus idempotency results grew during writes.

Current optimizations:

- Guards are precompiled during DSL compilation and only executed at runtime.
- Guard runtime environment uses a typed struct and reuses temporary objects.
- The in-memory test Repository uses structured keys.
- Benchmark setup and transition measurement are separated.
- Benchmark preallocates room for 100,000 logs and idempotency results to avoid growth noise.

## Guidance

Use the benchmark to track trends, not as a fixed performance guarantee. For production use, rerun it with your own DSL complexity, actions, storage implementation, database latency, and concurrency model.
