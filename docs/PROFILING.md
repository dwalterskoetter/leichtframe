# LeichtFrame Profiling Guide

This guide explains how to profile CPU usage, memory allocations, and GC behavior for LeichtFrame. It serves as a reference for maintaining the high-performance standards of the library.

## 🎯 Performance Baselines (Acceptance Criteria)

Any change to the Core engine must respect these baselines to ensure the "high-performance" promise is kept.

| Metric              | Scenario                       | Limit / Goal                                 |
| :------------------ | :----------------------------- | :------------------------------------------- |
| **Allocation**      | `IntColumn` Creation (1M rows) | **~0 Bytes** (Zero-Allocation via ArrayPool) |
| **Slicing**         | `Slice(start, length)`         | **< 10 ns** (O(1) / Zero-Copy)               |
| **Memory Overhead** | Overhead per Column            | **< 100 Bytes** (Class wrapper only)         |
| **NullBitmap**      | Set/Get Speed                  | **< 2x** of native `bool[]` time             |
| **Throughput**      | Random Access (Get/Set)        | **< 10 ns** per op (hot cache)               |

## 🛠 Tools (Linux/macOS/Windows)

We use standard .NET global tools. Ensure you have them installed:

```bash
dotnet tool install --global dotnet-trace
dotnet tool install --global dotnet-counters
dotnet tool install --global dotnet-gcdump
```

---

## CPU Profiling (dotnet-trace)

Use `dotnet-trace` to identify "hot paths" (methods that consume the most CPU time) or blocking calls.

**How to run**

Use the helper script to run the benchmarks with tracing enabled:

```bash
./scripts/profile_cpu.sh
```

**Analyze**

The output file (`traces/cpu_trace.nettrace`) can be analyzed in two ways:

- **Visual Studio (Windows):** Open the file directly.
- **Speedscope (Cross-platform):**

  1. Convert the trace:

```bash
dotnet-trace convert --format speedscope traces/cpu_trace.nettrace
```

2. Upload the resulting `.speedscope.json` to [https://speedscope.app](https://speedscope.app).

## Real-time GC Monitoring (dotnet-counters)

Use `dotnet-counters` to see Garbage Collection generations and heap size in real-time. This is crucial to verify "Zero-Allocation" claims.

**How to run**

1. Start your application or benchmark loop.
2. Find the Process ID (PID) using `dotnet-counters ps`.
3. Run the monitor script:

```bash
./scripts/monitor_gc.sh <PID>
```

**Key Metrics to watch**

- **GC Heap Size (MB):** Should remain stable. Continuous growth indicates a memory leak.
- **Gen 0 GC Count:** Should be low/zero during data processing phases (thanks to ArrayPool).
- **Allocation Rate:** Should be near zero for Core operations.

## Memory Heap Snapshots (dotnet-gcdump)

Use `dotnet-gcdump` to find memory leaks (objects that are not collected) or to inspect the object graph.

**How to run**

```bash
./scripts/capture_dump.sh <PID>
```

**Analyze**

Open the resulting `.gcdump` file in Visual Studio or VS Code (using the **.NET Install Tool** extension). You can compare two dumps to see which objects survived between snapshots.

## 🚑 Troubleshooting — Common Issues

- **High Gen 2 Collections:** This usually means large objects (>85KB) are being allocated frequently without pooling, or objects are living too long. Check ArrayPool usage.

- **Rising Heap Size:** If the heap grows indefinitely, check for `IDisposable` objects (Columns) that are not being disposed, preventing the ArrayPool from reclaiming arrays.

- **Slow Slicing:** Ensure you are using `ReadOnlyMemory<T>` or `Span<T>` and not copying data to new arrays.

### Quick tips

- Prefer pooling (ArrayPool) for large buffers.
- Use `ValueTask`/`ref struct` patterns where appropriate to avoid allocations.
- Add focused microbenchmarks (BenchmarkDotNet) for suspicious hot paths.
- When in doubt, capture a short `dotnet-trace` and inspect flame graphs in speedscope.

---

_Saved: docs/PROFILING.md_
