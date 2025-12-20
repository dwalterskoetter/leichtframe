using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;
using LeichtFrame.Core.Operations.Sort;

namespace LeichtFrame.Benchmarks
{
    public class SortingBenchmarks : BenchmarkData
    {
        // =========================================================
        // FULL SORTING (OrderBy)
        // =========================================================

        [Benchmark(Baseline = true, Description = "DuckDB Sort (Int)")]
        public void DuckDB_Sort_Int()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT * FROM BenchData ORDER BY Id";
            using var reader = cmd.ExecuteReader();

            // Iterate to ensure sorting is actually executed and materialized
            while (reader.Read()) { }
        }

        [Benchmark(Description = "LeichtFrame Sort (Int)")]
        public DataFrame LF_Sort_Int()
        {
            return _lfFrame.OrderBy("Id");
        }

        [Benchmark(Description = "DuckDB Sort (String)")]
        public void DuckDB_Sort_String()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT * FROM BenchData ORDER BY UniqueId";
            using var reader = cmd.ExecuteReader();

            while (reader.Read()) { }
        }

        [Benchmark(Description = "LeichtFrame Sort (String)")]
        public DataFrame LF_Sort_String()
        {
            return _lfFrame.OrderBy("UniqueId");
        }

        // =========================================================
        // TOP-N (Smallest/Largest vs LIMIT)
        // =========================================================

        [Benchmark(Description = "DuckDB Top 10 (Int)")]
        public void DuckDB_TopN_Int()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT * FROM BenchData ORDER BY Id ASC LIMIT 10";
            using var reader = cmd.ExecuteReader();

            while (reader.Read()) { }
        }

        [Benchmark(Description = "LeichtFrame Top 10 (Int)")]
        public DataFrame LF_TopN_Int()
        {
            // Uses optimized PriorityQueue implementation
            return _lfFrame.Smallest(10, "Id");
        }

        [Benchmark(Description = "DuckDB Top 10 (String)")]
        public void DuckDB_TopN_String()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT * FROM BenchData ORDER BY UniqueId DESC LIMIT 10";
            using var reader = cmd.ExecuteReader();

            while (reader.Read()) { }
        }

        [Benchmark(Description = "LeichtFrame Top 10 (String)")]
        public DataFrame LF_TopN_String()
        {
            return _lfFrame.Largest(10, "UniqueId");
        }
    }
}