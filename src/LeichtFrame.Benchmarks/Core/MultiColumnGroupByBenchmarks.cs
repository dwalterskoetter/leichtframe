using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;
using static LeichtFrame.Core.Expressions.F;

namespace LeichtFrame.Benchmarks
{
    [MemoryDiagnoser]
    public class MultiColumnGroupByBenchmarks : BenchmarkData
    {
        [Benchmark(Baseline = true, Description = "DuckDB GroupBy (2 Cols)")]
        public void DuckDB_Group2()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Category, (Id % 100) as SubGroup, COUNT(*) FROM BenchData GROUP BY Category, (Id % 100)";
            using var reader = cmd.ExecuteReader();
            while (reader.Read()) { }
        }

        [Benchmark(Description = "LF Lazy GroupBy (2 Cols)")]
        public DataFrame LF_Group2()
        {
            return _lfFrame.Lazy()
                .Aggregate(
                    new[] { Col("Category"), Col("Id") },
                    new[] { Count().As("Cnt") }
                )
                .Collect();
        }
    }
}