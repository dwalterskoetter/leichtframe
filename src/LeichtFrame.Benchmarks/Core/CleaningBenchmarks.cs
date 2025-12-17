using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;

namespace LeichtFrame.Benchmarks
{
    public class CleaningBenchmarks : BenchmarkData
    {
        public override void GlobalSetup()
        {
            base.GlobalSetup();

            // 1. Inject Nulls into LeichtFrame
            // We set every 5th row of 'Category' to null (~20% null rate)
            var catCol = (StringColumn)_lfFrame["Category"];
            for (int i = 0; i < N; i += 5)
            {
                catCol.SetNull(i);
            }

            // 2. Inject Nulls into DuckDB to match the state
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "UPDATE BenchData SET Category = NULL WHERE (Id % 5) = 0";
            cmd.ExecuteNonQuery();
        }

        // =========================================================
        // DISTINCT
        // =========================================================

        [Benchmark(Baseline = true, Description = "DuckDB Distinct")]
        public long DuckDB_Distinct()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT DISTINCT Category FROM BenchData";
            using var reader = cmd.ExecuteReader();

            long count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LeichtFrame Distinct")]
        public DataFrame LF_Distinct()
        {
            return _lfFrame.Distinct("Category");
        }

        // =========================================================
        // DROP NULLS
        // =========================================================

        [Benchmark(Description = "DuckDB DropNulls (Filter)")]
        public long DuckDB_DropNulls()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT * FROM BenchData WHERE Category IS NOT NULL";
            using var reader = cmd.ExecuteReader();

            long count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LeichtFrame DropNulls")]
        public DataFrame LF_DropNulls()
        {
            // Removes rows where ANY column is null (checks all columns)
            return _lfFrame.DropNulls();
        }

        // =========================================================
        // FILL NULL (Coalesce)
        // =========================================================

        [Benchmark(Description = "DuckDB FillNull (Coalesce)")]
        public long DuckDB_FillNull()
        {
            using var cmd = _duckConnection.CreateCommand();
            // Simulating creating a new projection with filled values
            cmd.CommandText = "SELECT COALESCE(Category, 'MISSING') FROM BenchData";
            using var reader = cmd.ExecuteReader();

            long count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LeichtFrame FillNull")]
        public DataFrame LF_FillNull()
        {
            return _lfFrame.FillNull("Category", "MISSING");
        }
    }
}