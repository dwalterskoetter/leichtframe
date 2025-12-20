using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;
using LeichtFrame.Core.Operations.GroupBy;
using LeichtFrame.Core.Operations.Aggregate;
using static LeichtFrame.Core.Expressions.F;

namespace LeichtFrame.Benchmarks
{
    [MemoryDiagnoser]
    public class LazyVsEagerGroupByBenchmarks : BenchmarkData
    {
        [Benchmark(Baseline = true, Description = "DuckDB GroupBy")]
        public long DuckDB_Group()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Id, COUNT(*) FROM BenchData GROUP BY Id";

            using var reader = cmd.ExecuteReader();
            long count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LF Eager (Direct API)")]
        public DataFrame LF_Eager()
        {
            return _lfFrame.GroupBy("Id").Count();
        }

        [Benchmark(Description = "LF Lazy (Expression API)")]
        public DataFrame LF_Lazy()
        {
            // FIX: Updated to use .Agg() syntax
            return _lfFrame.Lazy()
                           .GroupBy("Id")
                           .Agg(Count().As("Count"))
                           .Collect();
        }
    }
}