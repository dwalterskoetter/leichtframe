using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;
using DuckDB.NET.Data;

namespace LeichtFrame.Benchmarks
{
    public class GroupByBenchmarks : BenchmarkData
    {
        // --- Low Cardinality ---

        [Benchmark(Baseline = true, Description = "DuckDB GroupBy (LowCard)")]
        public long DuckDB_Group_Low()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Category, COUNT(*) FROM BenchData GROUP BY Category";
            using var reader = cmd.ExecuteReader();

            long count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LeichtFrame GroupBy (LowCard)")]
        public DataFrame LF_Group_Low()
        {
            return _lfFrame.GroupBy("Category").Count();
        }

        // --- High Cardinality ---

        [Benchmark(Description = "DuckDB GroupBy (HighCard)")]
        public long DuckDB_Group_High()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT UniqueId, COUNT(*) FROM BenchData GROUP BY UniqueId";
            using var reader = cmd.ExecuteReader();

            long count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LeichtFrame GroupBy (HighCard)")]
        public DataFrame LF_Group_High()
        {
            return _lfFrame.GroupBy("UniqueId").Count();
        }

        // --- Aggregation Calculation (Sum) ---

        [Benchmark(Description = "DuckDB Sum GroupBy")]
        public double DuckDB_Group_Sum()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Category, SUM(Val) FROM BenchData GROUP BY Category";
            using var reader = cmd.ExecuteReader();

            double sumCheck = 0;
            while (reader.Read()) sumCheck += reader.GetDouble(1);
            return sumCheck;
        }

        [Benchmark(Description = "LeichtFrame Sum GroupBy")]
        public DataFrame LF_Group_Sum()
        {
            return _lfFrame.GroupBy("Category").Sum("Val");
        }
    }
}