using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;

namespace LeichtFrame.Benchmarks
{
    [MemoryDiagnoser]
    public class NullHandlingBenchmarks
    {
        [Params(1_000_000)]
        public int N;

        private NullBitmap _bitmap = null!;
        private bool[] _boolArray = null!;

        [GlobalSetup]
        public void Setup()
        {
            _bitmap = new NullBitmap(N);
            _boolArray = new bool[N];
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _bitmap.Dispose();
        }

        [Benchmark(Baseline = true)]
        public void BoolArray_Set()
        {
            // Simulate access to every 100th position
            for (int i = 0; i < N; i += 100)
            {
                _boolArray[i] = true;
            }
        }

        [Benchmark]
        public void NullBitmap_Set()
        {
            for (int i = 0; i < N; i += 100)
            {
                _bitmap.SetNull(i);
            }
        }
    }
}