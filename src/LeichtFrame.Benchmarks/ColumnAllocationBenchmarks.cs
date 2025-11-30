using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;

namespace LeichtFrame.Benchmarks
{
    [MemoryDiagnoser] // Important: Measures RAM usage & GC
    [ShortRunJob]     // FFor faster testing during development (use "DefaultJob" in release)
    public class ColumnAllocationBenchmarks
    {
        [Params(100_000, 1_000_000)] // Test with 100k and 1M rows
        public int N;

        [Benchmark(Baseline = true)]
        public int[] NativeArrayAllocation()
        {
            // Standard .NET behavior: Always new heap memory
            return new int[N];
        }

        [Benchmark]
        public void LeichtFrame_IntColumn()
        {
            // Your code: Should use ArrayPool
            using var col = new IntColumn("Bench", N);
            // We simulate usage so the JIT doesn't optimize it away
            col.Append(42);
        }
    }
}