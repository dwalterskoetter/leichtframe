using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;
using LeichtFrame.IO;
using static LeichtFrame.Core.Expressions.F;
using LeichtFrame.Core.Operations.Transform;

namespace LeichtFrame.Benchmarks
{
    [MemoryDiagnoser]
    [Orderer(BenchmarkDotNet.Order.SummaryOrderPolicy.FastestToSlowest)]
    public class GroupByBreakdownBenchmarks
    {
        private DataFrame _filteredDf = null!;
        private readonly DateTime _targetDate = new DateTime(1998, 9, 2);
        private const string FilePath = "/home/dennis/source/repos/dbgen/tpch-dbgen/lineitem.tbl";

        [GlobalSetup]
        public void Setup()
        {
            if (!File.Exists(FilePath)) throw new FileNotFoundException(FilePath);

            var schema = new DataFrameSchema(new[]
            {
                new ColumnDefinition("l_returnflag", typeof(string), SourceIndex: 8),
                new ColumnDefinition("l_linestatus", typeof(string), SourceIndex: 9),
                new ColumnDefinition("l_quantity", typeof(double), SourceIndex: 4),
            });

            using var raw = CsvReader.Read(FilePath, schema, new CsvReadOptions { Separator = "|", HasHeader = false, HasTrailingDelimiter = true });

            _filteredDf = raw.Where(row => true);
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _filteredDf?.Dispose();
        }

        // =========================================================
        // TEST A: Single Column Grouping
        // =========================================================
        [Benchmark(Description = "A: GroupBy Single Col (Flag)")]
        public void GroupSingle()
        {
            using var res = _filteredDf.Lazy()
                .GroupBy("l_returnflag")
                .Agg(Count().As("Cnt"))
                .Collect();
        }

        // =========================================================
        // TEST B: Multi-Column Grouping
        // =========================================================
        [Benchmark(Description = "B: GroupBy Multi Col (Flag, Status)")]
        public void GroupMulti()
        {
            using var res = _filteredDf.Lazy()
                .GroupBy("l_returnflag", "l_linestatus")
                .Agg(Count().As("Cnt"))
                .Collect();
        }

        // =========================================================
        // TEST C: Manual String Concatenation (Simulation)
        // =========================================================
        [Benchmark(Description = "C: Manual String Concat Grouping")]
        public void GroupManualConcat()
        {
            var withKey = _filteredDf.AddColumn("ManualKey", row =>
                row.Get<string>("l_returnflag") + "_" + row.Get<string>("l_linestatus"));

            using var res = withKey.Lazy()
                .GroupBy("ManualKey")
                .Agg(Count().As("Cnt"))
                .Collect();
        }

        // =========================================================
        // TEST D: Multi-Column with Payload (Full Aggregation)
        // =========================================================
        [Benchmark(Description = "D: GroupBy Multi + Sum(Qty)")]
        public void GroupMultiWithAgg()
        {
            using var res = _filteredDf.Lazy()
                .GroupBy("l_returnflag", "l_linestatus")
                .Agg(
                    Count().As("Cnt"),
                    Sum(Col("l_quantity")).As("SumQty")
                )
                .Collect();
        }
    }
}