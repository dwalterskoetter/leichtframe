using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using LeichtFrame.Core;
using LeichtFrame.Core.Operations.Aggregate;
using LeichtFrame.Core.Operations.GroupBy;

namespace LeichtFrame.Benchmarks.Breakdown
{
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    public class LF_Pipeline_Breakdown
    {
        [Params(1_000_000)]
        public int N;

        private DataFrame _stringDf = null!;

        private GroupedDataFrame _preparedGdf = null!;

        [GlobalSetup]
        public void Setup()
        {
            var cats = new[] { "A", "B", "C", "D", "E" };
            var rnd = new Random(42);
            var colCat = new StringColumn("Category", N);
            var colVal = new DoubleColumn("Val", N);

            for (int i = 0; i < N; i++)
            {
                colCat.Append(cats[rnd.Next(cats.Length)]);
                colVal.Append(rnd.NextDouble());
            }

            _stringDf = new DataFrame(new IColumn[] { colCat, colVal });
            _preparedGdf = _stringDf.GroupBy("Category");
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _stringDf?.Dispose();
            _preparedGdf?.Dispose();
        }

        // =========================================================
        // STEP 1: GROUPING ONLY (Eager)
        // =========================================================

        [Benchmark(Description = "1. Grouping Phase")]
        public object Step1_GroupBy()
        {
            using var gdf = _stringDf.GroupBy("Category");
            return gdf.GroupCount;
        }

        // =========================================================
        // STEP 2: STREAM CREATION ONLY
        // =========================================================

        [Benchmark(Description = "2. Plan Stream")]
        public object Step2_CreateStream()
        {
            return _stringDf.Lazy()
                            .GroupBy("Category")
                            .Agg("Val".Sum().As("Total"))
                            .CollectStream();
        }

        // =========================================================
        // STEP 3: ITERATION ONLY
        // =========================================================

        [Benchmark(Description = "3. Iteration Phase")]
        public int Step3_Iterate()
        {
            int sum = 0;
            foreach (var (key, count) in _preparedGdf.CountStream())
            {
                sum += count;
            }
            return sum;
        }

        // =========================================================
        // TOTAL
        // =========================================================

        [Benchmark(Description = "4. Total Pipeline (LF_C_Stream)")]
        public double Total()
        {
            var stream = _stringDf.Lazy()
                                 .GroupBy("Category")
                                 .Agg("Val".Sum().As("Total"))
                                 .CollectStream();

            double checkSum = 0;
            foreach (var row in stream)
            {
                string cat = row.Get<string>(0);
                double sum = row.Get<double>(1);
                checkSum += sum + cat.Length;
            }
            return checkSum;
        }
    }
}