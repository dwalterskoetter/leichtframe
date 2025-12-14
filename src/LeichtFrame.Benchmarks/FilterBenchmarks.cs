using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;
using DuckDB.NET.Data;

namespace LeichtFrame.Benchmarks
{
    public class FilterBenchmarks : BenchmarkData
    {
        [Benchmark(Baseline = true, Description = "DuckDB Filter (Iterate)")]
        public long DuckDB_Filter()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT * FROM BenchData WHERE Id < 50000";
            using var reader = cmd.ExecuteReader();

            long count = 0;

            while (reader.Read())
            {
                count++;
            }
            return count;
        }

        [Benchmark(Description = "LeichtFrame Where (Delegate)")]
        public DataFrame LF_Where()
        {
            return _lfFrame.Where(row => row.Get<int>("Id") < 50_000);
        }

        [Benchmark(Description = "LeichtFrame Where (Vectorized)")]
        public DataFrame LF_WhereVec()
        {
            return _lfFrame.WhereVec("Id", CompareOp.LessThan, 50_000);
        }
    }
}