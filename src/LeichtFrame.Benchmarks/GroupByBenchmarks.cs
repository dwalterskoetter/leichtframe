using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;
using DuckDB.NET.Data;

namespace LeichtFrame.Benchmarks
{
    public class GroupByBenchmarks : BenchmarkData
    {
        // =========================================================
        // COUNT (Low & High Cardinality)
        // =========================================================

        [Benchmark(Baseline = true, Description = "DuckDB GroupBy Count (LowCard)")]
        public long DuckDB_Group_Count_Low()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Category, COUNT(*) FROM BenchData GROUP BY Category";
            using var reader = cmd.ExecuteReader();

            long count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LeichtFrame GroupBy Count (LowCard)")]
        public DataFrame LF_Group_Count_Low()
        {
            return _lfFrame.GroupBy("Category").Count();
        }

        [Benchmark(Description = "DuckDB GroupBy Count (HighCard)")]
        public long DuckDB_Group_Count_High()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT UniqueId, COUNT(*) FROM BenchData GROUP BY UniqueId";
            using var reader = cmd.ExecuteReader();

            long count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LeichtFrame GroupBy Count (HighCard)")]
        public DataFrame LF_Group_Count_High()
        {
            return _lfFrame.GroupBy("UniqueId").Count();
        }

        // =========================================================
        // SUM
        // =========================================================

        [Benchmark(Description = "DuckDB GroupBy Sum")]
        public double DuckDB_Group_Sum()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Category, SUM(Val) FROM BenchData GROUP BY Category";
            using var reader = cmd.ExecuteReader();

            double sumCheck = 0;
            while (reader.Read()) sumCheck += reader.GetDouble(1);
            return sumCheck;
        }

        [Benchmark(Description = "LeichtFrame GroupBy Sum")]
        public DataFrame LF_Group_Sum()
        {
            return _lfFrame.GroupBy("Category").Sum("Val");
        }

        // =========================================================
        // MEAN
        // =========================================================

        [Benchmark(Description = "DuckDB GroupBy Mean")]
        public double DuckDB_Group_Mean()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Category, AVG(Val) FROM BenchData GROUP BY Category";
            using var reader = cmd.ExecuteReader();

            double check = 0;
            while (reader.Read()) check += reader.GetDouble(1);
            return check;
        }

        [Benchmark(Description = "LeichtFrame GroupBy Mean")]
        public DataFrame LF_Group_Mean()
        {
            return _lfFrame.GroupBy("Category").Mean("Val");
        }

        // =========================================================
        // MIN
        // =========================================================

        [Benchmark(Description = "DuckDB GroupBy Min")]
        public double DuckDB_Group_Min()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Category, MIN(Val) FROM BenchData GROUP BY Category";
            using var reader = cmd.ExecuteReader();

            double check = 0;
            while (reader.Read()) check += reader.GetDouble(1);
            return check;
        }

        [Benchmark(Description = "LeichtFrame GroupBy Min")]
        public DataFrame LF_Group_Min()
        {
            return _lfFrame.GroupBy("Category").Min("Val");
        }

        // =========================================================
        // MAX
        // =========================================================

        [Benchmark(Description = "DuckDB GroupBy Max")]
        public double DuckDB_Group_Max()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Category, MAX(Val) FROM BenchData GROUP BY Category";
            using var reader = cmd.ExecuteReader();

            double check = 0;
            while (reader.Read()) check += reader.GetDouble(1);
            return check;
        }

        [Benchmark(Description = "LeichtFrame GroupBy Max")]
        public DataFrame LF_Group_Max()
        {
            return _lfFrame.GroupBy("Category").Max("Val");
        }

        // =========================================================
        // STRING GROUPBY
        // =========================================================

        [Benchmark(Description = "DuckDB GroupBy String")]
        public long DuckDB_String()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT UniqueId, COUNT(*) FROM BenchData GROUP BY UniqueId";
            using var reader = cmd.ExecuteReader();

            long count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LeichtFrame GroupBy String (Parallel)")]
        public DataFrame LF_String()
        {
            // Das triggert jetzt den neuen GroupByStringParallel Pfad (da N >= 100k)
            return _lfFrame.GroupBy("UniqueId").Count();
        }
    }
}