using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;

namespace LeichtFrame.Benchmarks
{
    [MemoryDiagnoser]
    public class SlicingBenchmarks
    {
        private IntColumn _col = null!;

        [Params(1_000_000)]
        public int N;

        [GlobalSetup]
        public void Setup()
        {
            _col = new IntColumn("SliceBench", N);
            for (int i = 0; i < N; i++) _col.Append(i);
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _col.Dispose();
        }

        [Benchmark]
        public ReadOnlyMemory<int> CreateSlice()
        {
            // This here must be extremely fast (ns range)
            return _col.Slice(0, N / 2);
        }
    }
}