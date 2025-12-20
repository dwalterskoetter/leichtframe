using BenchmarkDotNet.Attributes;
using LeichtFrame.Core.Operations.Aggregate;

namespace LeichtFrame.Benchmarks
{
    public class AggregationBenchmarks : BenchmarkData
    {
        // =========================================================
        // SUM
        // =========================================================

        [Benchmark(Baseline = true, Description = "DuckDB Sum")]
        public double DuckDB_Sum()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT SUM(Val) FROM BenchData";
            return (double)cmd.ExecuteScalar()!;
        }

        [Benchmark(Description = "LeichtFrame Sum")]
        public double LF_Sum()
        {
            return _lfFrame.Sum("Val");
        }

        // =========================================================
        // MEAN
        // =========================================================

        [Benchmark(Description = "DuckDB Mean")]
        public double DuckDB_Mean()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT AVG(Val) FROM BenchData";
            return (double)cmd.ExecuteScalar()!;
        }

        [Benchmark(Description = "LeichtFrame Mean")]
        public double LF_Mean()
        {
            return _lfFrame.Mean("Val");
        }

        // =========================================================
        // MIN
        // =========================================================

        [Benchmark(Description = "DuckDB Min")]
        public double DuckDB_Min()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT MIN(Val) FROM BenchData";
            return (double)cmd.ExecuteScalar()!;
        }

        [Benchmark(Description = "LeichtFrame Min")]
        public double LF_Min()
        {
            return _lfFrame.Min("Val");
        }

        // =========================================================
        // MAX
        // =========================================================

        [Benchmark(Description = "DuckDB Max")]
        public double DuckDB_Max()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT MAX(Val) FROM BenchData";
            return (double)cmd.ExecuteScalar()!;
        }

        [Benchmark(Description = "LeichtFrame Max")]
        public double LF_Max()
        {
            return _lfFrame.Max("Val");
        }
    }
}