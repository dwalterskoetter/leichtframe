using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using LeichtFrame.Core;
using LeichtFrame.Core.Engine;
using LeichtFrame.Core.Engine.Algorithms.Converter;
using LeichtFrame.Core.Operations.Aggregate;
using LeichtFrame.Core.Operations.GroupBy;

namespace LeichtFrame.Benchmarks.Breakdown
{
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    public class LowCardStreamingBreakdownBenchmarks
    {
        [Params(1_000_000)]
        public int N;

        private DataFrame _stringDf = null!;
        private CategoryColumn _preConvertedCol = null!;

        // Wir nutzen eine Wrapper-Klasse, um das Dispose zu verhindern
        private NonDisposableGroupedDataFrame _preGroupedGdf = null!;

        [GlobalSetup]
        public void Setup()
        {
            var cats = new[] { "A", "B", "C", "D", "E" };
            var rnd = new Random(42);
            var colCat = new StringColumn("Category", N);

            for (int i = 0; i < N; i++)
            {
                colCat.Append(cats[rnd.Next(cats.Length)]);
            }

            _stringDf = new DataFrame(new IColumn[] { colCat });

            _preConvertedCol = ParallelStringConverter.Convert(colCat);

            var strategy = new DirectAddressingStrategy(0, _preConvertedCol.Cardinality);
            var nativeData = strategy.ComputeNative(_preConvertedCol.Codes, N);

            // Wrapper instanziieren
            _preGroupedGdf = new NonDisposableGroupedDataFrame(
                _stringDf,
                new[] { "Category" },
                nativeData
            );
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _stringDf?.Dispose();
            _preConvertedCol?.Dispose();
            // Manuelles Dispose aufrufen
            _preGroupedGdf?.RealDispose();
        }

        // --- Helper Class ---
        private class NonDisposableGroupedDataFrame : GroupedDataFrame<int>
        {
            public NonDisposableGroupedDataFrame(DataFrame df, string[] cols, NativeGroupedData native)
                : base(df, cols, native) { }

            public override void Dispose()
            {
                // Do nothing to keep NativeData alive for benchmark iterations
            }

            public void RealDispose()
            {
                base.Dispose();
            }
        }

        // =========================================================

        [Benchmark(Description = "1. Converter Only")]
        public void Step1_Converter()
        {
            var strCol = (StringColumn)_stringDf["Category"];
            using var cat = ParallelStringConverter.Convert(strCol);
        }

        [Benchmark(Description = "2. Grouping Strategy Only")]
        public void Step2_Grouping()
        {
            var strategy = new DirectAddressingStrategy(0, _preConvertedCol.Cardinality);
            using var nativeData = strategy.ComputeNative(_preConvertedCol.Codes, N);
        }

        [Benchmark(Description = "3. Enumerator Only")]
        public int Step3_Streaming()
        {
            int sum = 0;
            foreach (var (key, count) in _preGroupedGdf.CountStream())
            {
                sum += count;
            }
            return sum;
        }

        [Benchmark(Description = "Total: Auto-Detect Pipeline")]
        public int Total_Pipeline()
        {
            int sum = 0;
            using var gdf = _stringDf.GroupBy("Category");
            foreach (var (key, count) in gdf.CountStream())
            {
                sum += count;
            }
            return sum;
        }
    }
}