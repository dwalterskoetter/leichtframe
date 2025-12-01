using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;

namespace LeichtFrame.Benchmarks
{
    [MemoryDiagnoser]
    public class FilterBenchmarks
    {
        [Params(1_000_000)]
        public int N;

        private DataFrame _df = null!;
        private List<int> _list = null!;

        [GlobalSetup]
        public void Setup()
        {
            // 1. Setup LeichtFrame
            var schema = new DataFrameSchema(new[] { new ColumnDefinition("Val", typeof(int)) });
            _df = DataFrame.Create(schema, N);
            var col = (IntColumn)_df["Val"];

            // 2. Setup LINQ List
            _list = new List<int>(N);

            for (int i = 0; i < N; i++)
            {
                col.Append(i);
                _list.Add(i);
            }
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _df.Dispose();
        }

        // Baseline: Standard C# LINQ
        [Benchmark(Baseline = true)]
        public List<int> Linq_Where_ToList()
        {
            // Filter: All values > N/2
            return _list.Where(x => x > N / 2).ToList();
        }

        // Your candidate
        [Benchmark]
        public DataFrame LeichtFrame_Where()
        {
            // Same filter
            int threshold = N / 2;
            return _df.Where(row => row.Get<int>(0) > threshold);
        }
    }
}