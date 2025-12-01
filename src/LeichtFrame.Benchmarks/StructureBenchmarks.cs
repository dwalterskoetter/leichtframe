using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;

namespace LeichtFrame.Benchmarks
{
    [MemoryDiagnoser]
    public class StructureBenchmarks
    {
        [Params(10_000, 1_000_000)] // Test small and large to prove O(1)
        public int N;

        private DataFrame _df = null!;

        [GlobalSetup]
        public void Setup()
        {
            // 1. Define schema
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Col1", typeof(int)),
                new ColumnDefinition("Col2", typeof(int)),
                new ColumnDefinition("Col3", typeof(int)),
                new ColumnDefinition("Col4", typeof(int)),
                new ColumnDefinition("Col5", typeof(int))
            });

            // 2. Create DataFrame
            _df = DataFrame.Create(schema, N);

            // 3. Fill ALL columns
            // So that all have length N and no "Length Mismatch" occurs.
            foreach (var col in _df.Columns)
            {
                var intCol = (IntColumn)col;
                for (int i = 0; i < N; i++)
                {
                    intCol.Append(i);
                }
            }
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _df.Dispose();
        }

        // Scenario: Column Access
        [Benchmark]
        public IColumn Indexer_ByName()
        {
            return _df["Col3"]; // Should be O(1) dictionary lookup
        }

        // Scenario: Projection (Select)
        [Benchmark]
        public DataFrame Select_TwoColumns()
        {
            // Should be extremely fast and only copy metadata (Shallow Copy)
            return _df.Select("Col1", "Col5");
        }

        // Scenario: Slicing (Head)
        [Benchmark]
        public DataFrame Head_100()
        {
            // Should create SlicedColumn wrapper, no data copying
            return _df.Head(100);
        }

        // Scenario: Slicing (Middle)
        [Benchmark]
        public DataFrame Slice_Middle()
        {
            // In the middle
            return _df.Slice(N / 2, 1000);
        }
    }
}