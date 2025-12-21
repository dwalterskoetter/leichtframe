using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;
using LeichtFrame.IO;
using static LeichtFrame.Core.Expressions.F;

namespace LeichtFrame.Benchmarks
{
    [MemoryDiagnoser]
    [Orderer(BenchmarkDotNet.Order.SummaryOrderPolicy.FastestToSlowest)]
    public class TPCHBreakdownBenchmarks
    {
        private DataFrame _lineItemDf = null!;
        private readonly DateTime _targetDate = new DateTime(1998, 9, 2);
        private const string FilePath = "/home/dennis/source/repos/dbgen/tpch-dbgen/lineitem.tbl";

        [GlobalSetup]
        public void Setup()
        {
            if (!File.Exists(FilePath)) throw new FileNotFoundException(FilePath);

            var schema = new DataFrameSchema(new[]
            {
                new ColumnDefinition("l_quantity", typeof(double), SourceIndex: 4),
                new ColumnDefinition("l_extendedprice", typeof(double), SourceIndex: 5),
                new ColumnDefinition("l_discount", typeof(double), SourceIndex: 6),
                new ColumnDefinition("l_tax", typeof(double), SourceIndex: 7),
                new ColumnDefinition("l_returnflag", typeof(string), SourceIndex: 8),
                new ColumnDefinition("l_linestatus", typeof(string), SourceIndex: 9),
                new ColumnDefinition("l_shipdate", typeof(DateTime), SourceIndex: 10)
            });

            _lineItemDf = CsvReader.Read(FilePath, schema, new CsvReadOptions { Separator = "|", HasHeader = false, HasTrailingDelimiter = true });
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _lineItemDf?.Dispose();
        }

        // =========================================================
        // STEP 1: Filter
        // Measures: WhereVec (SIMD) + CloneSubset (Raw Byte Copy)
        // =========================================================
        [Benchmark(Description = "1. Filter Only")]
        public void Step1_Filter()
        {
            using var res = _lineItemDf.Lazy()
                .Where(Col("l_shipdate") <= _targetDate)
                .Collect();
        }

        // =========================================================
        // STEP 2: Filter + Projection (Calculation)
        // Measures: Evaluator JIT Kernel Fusion + Result Allocations
        // =========================================================
        [Benchmark(Description = "2. Filter + Calc")]
        public void Step2_Filter_Calc()
        {
            using var res = _lineItemDf.Lazy()
                .Where(Col("l_shipdate") <= _targetDate)
                .Select(
                    Col("l_returnflag"),
                    Col("l_linestatus"),
                    Col("l_quantity"),
                    Col("l_extendedprice"),
                    Col("l_discount"),
                    (Col("l_extendedprice") * (1.0 - Col("l_discount"))).As("disc_price"),
                    (Col("l_extendedprice") * (1.0 - Col("l_discount")) * (1.0 + Col("l_tax"))).As("charge")
                )
                .Collect();
        }

        // =========================================================
        // STEP 3: Filter + Calc + GroupBy + Agg
        // Measures: MultiColumn Hashing + Aggregation (Sum/Mean)
        // =========================================================
        [Benchmark(Description = "3. Filter + Calc + GroupAgg")]
        public void Step3_Filter_Calc_GroupAgg()
        {
            using var res = _lineItemDf.Lazy()
                .Where(Col("l_shipdate") <= _targetDate)
                .Select(
                    Col("l_returnflag"),
                    Col("l_linestatus"),
                    Col("l_quantity"),
                    Col("l_extendedprice"),
                    Col("l_discount"),
                    (Col("l_extendedprice") * (1.0 - Col("l_discount"))).As("disc_price"),
                    (Col("l_extendedprice") * (1.0 - Col("l_discount")) * (1.0 + Col("l_tax"))).As("charge")
                )
                .GroupBy("l_returnflag", "l_linestatus")
                .Agg(
                    Sum(Col("l_quantity")).As("sum_qty"),
                    Sum(Col("l_extendedprice")).As("sum_base_price"),
                    Sum(Col("disc_price")).As("sum_disc_price"),
                    Sum(Col("charge")).As("sum_charge"),
                    Mean(Col("l_quantity")).As("avg_qty"),
                    Mean(Col("l_extendedprice")).As("avg_price"),
                    Mean(Col("l_discount")).As("avg_disc"),
                    Count().As("count_order")
                )
                .Collect();
        }

        // =========================================================
        // STEP 4: Full Query (incl. Sort)
        // Measures: Multi-Column Sort on Result
        // =========================================================
        [Benchmark(Description = "4. Full Query (Sort)")]
        public void Step4_Full()
        {
            using var res = _lineItemDf.Lazy()
                .Where(Col("l_shipdate") <= _targetDate)
                .Select(
                    Col("l_returnflag"),
                    Col("l_linestatus"),
                    Col("l_quantity"),
                    Col("l_extendedprice"),
                    Col("l_discount"),
                    (Col("l_extendedprice") * (1.0 - Col("l_discount"))).As("disc_price"),
                    (Col("l_extendedprice") * (1.0 - Col("l_discount")) * (1.0 + Col("l_tax"))).As("charge")
                )
                .GroupBy("l_returnflag", "l_linestatus")
                .Agg(
                    Sum(Col("l_quantity")).As("sum_qty"),
                    Sum(Col("l_extendedprice")).As("sum_base_price"),
                    Sum(Col("disc_price")).As("sum_disc_price"),
                    Sum(Col("charge")).As("sum_charge"),
                    Mean(Col("l_quantity")).As("avg_qty"),
                    Mean(Col("l_extendedprice")).As("avg_price"),
                    Mean(Col("l_discount")).As("avg_disc"),
                    Count().As("count_order")
                )
                .OrderBy("l_returnflag", "l_linestatus")
                .Collect();
        }
    }
}