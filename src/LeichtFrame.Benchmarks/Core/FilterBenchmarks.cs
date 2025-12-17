using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;
using DuckDB.NET.Data;

namespace LeichtFrame.Benchmarks
{
    public class FilterBenchmarks : BenchmarkData
    {
        // =========================================================
        // INTEGER FILTER (Numeric)
        // =========================================================

        [Benchmark(Baseline = true, Description = "DuckDB Filter (Int)")]
        public long DuckDB_Filter_Int()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT * FROM BenchData WHERE Id < 50000";
            using var reader = cmd.ExecuteReader();

            long count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LeichtFrame Where (Int - Delegate)")]
        public DataFrame LF_Where_Int()
        {
            return _lfFrame.Where(row => row.Get<int>("Id") < 50_000);
        }

        [Benchmark(Description = "LeichtFrame WhereView (Zero-Copy)")]
        public DataFrame LF_WhereView_Int()
        {
            return _lfFrame.WhereView(row => row.Get<int>("Id") < 50_000);
        }

        [Benchmark(Description = "LeichtFrame WhereVec (Int - SIMD)")]
        public DataFrame LF_WhereVec_Int()
        {
            // Hardware accelerated filtering
            return _lfFrame.WhereVec("Id", CompareOp.LessThan, 50_000);
        }

        // =========================================================
        // STRING FILTER (Text comparison)
        // =========================================================

        [Benchmark(Description = "DuckDB Filter (String)")]
        public long DuckDB_Filter_String()
        {
            using var cmd = _duckConnection.CreateCommand();
            // Category has values "A", "B", "C", "D", "E"
            cmd.CommandText = "SELECT * FROM BenchData WHERE Category = 'A'";
            using var reader = cmd.ExecuteReader();

            long count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LeichtFrame Where (String)")]
        public DataFrame LF_Where_String()
        {
            // Vectorized string filtering is not yet implemented, so we use the delegate approach
            return _lfFrame.Where(row => row.Get<string>("Category") == "A");
        }

        // =========================================================
        // COMPOUND FILTER (Multi-Column)
        // =========================================================

        [Benchmark(Description = "DuckDB Filter (Compound)")]
        public long DuckDB_Filter_Compound()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT * FROM BenchData WHERE Id < 10000 AND Category = 'A'";
            using var reader = cmd.ExecuteReader();

            long count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LeichtFrame Where (Compound)")]
        public DataFrame LF_Where_Compound()
        {
            // Accessing multiple columns per row increases overhead
            return _lfFrame.Where(row =>
                row.Get<int>("Id") < 10_000 &&
                row.Get<string>("Category") == "A"
            );
        }
    }
}