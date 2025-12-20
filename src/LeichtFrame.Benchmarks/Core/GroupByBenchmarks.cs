using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;
using LeichtFrame.Core.Operations.GroupBy;
using LeichtFrame.Core.Operations.Aggregate;

namespace LeichtFrame.Benchmarks
{
    public class GroupByBenchmarks : BenchmarkData
    {

        // =========================================================
        // COUNT (Low Cardinality INT)
        // =========================================================

        [Benchmark(Description = "DuckDB: GroupBy -> Reader (Stream)")]
        public long DuckDB_Reader()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Id, COUNT(*) FROM BenchData GROUP BY Id";

            using var reader = cmd.ExecuteReader();
            long total = 0;

            while (reader.Read())
            {
                int k = reader.GetInt32(0);
                int c = reader.GetInt32(1);
                total++;
            }
            return total;
        }

        [Benchmark(Description = "DuckDB GroupBy Count (Int)")]
        public long DuckDB_Group_Count_Low()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Id, COUNT(*) FROM BenchData GROUP BY Id";
            using var reader = cmd.ExecuteReader();

            long count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LF: GroupBy -> GetCountReader (Raw)")]
        public long LF_Stream_Raw()
        {
            using var gdf = _lfFrame.GroupBy("Id");
            var reader = gdf.GetCountReader();

            long total = 0;
            while (reader.Read(out int key, out int count))
            {
                total++;
            }
            return total;
        }

        [Benchmark(Description = "LF: GroupBy -> CountStream (Fluent)")]
        public long LF_Stream_Fluent()
        {
            long total = 0;

            foreach (var (key, count) in _lfFrame.GroupBy("Id").CountStream())
            {
                total++;
            }
            return total;
        }

        [Benchmark(Description = "DuckDB: GroupBy -> List (Materialized)")]
        public List<(int, int)> DuckDB_Materialized()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Id, COUNT(*) FROM BenchData GROUP BY Id";
            using var reader = cmd.ExecuteReader();

            var result = new List<(int, int)>();

            while (reader.Read())
            {
                result.Add((reader.GetInt32(0), reader.GetInt32(1)));
            }

            return result;
        }

        [Benchmark(Description = "LF: GroupBy -> Count (DataFrame)")]
        public DataFrame LF_Materialized()
        {
            return _lfFrame.GroupBy("Id").Count();
        }

        // =========================================================
        // COUNT (Low & High Cardinality STRING)
        // =========================================================

        // [Benchmark(Baseline = true, Description = "DuckDB GroupBy Count (LowCard)")]
        // public long DuckDB_Group_Count_Low_String()
        // {
        //     using var cmd = _duckConnection.CreateCommand();
        //     cmd.CommandText = "SELECT Category, COUNT(*) FROM BenchData GROUP BY Category";
        //     using var reader = cmd.ExecuteReader();

        //     long count = 0;
        //     while (reader.Read()) count++;
        //     return count;
        // }

        // [Benchmark(Description = "LeichtFrame GroupBy Count (LowCard)")]
        // public DataFrame LF_Group_Count_Low_String()
        // {
        //     return _lfFrame.GroupBy("Category").Count();
        // }

        // [Benchmark(Description = "DuckDB GroupBy Count (HighCard)")]
        // public long DuckDB_Group_Count_High()
        // {
        //     using var cmd = _duckConnection.CreateCommand();
        //     cmd.CommandText = "SELECT UniqueId, COUNT(*) FROM BenchData GROUP BY UniqueId";
        //     using var reader = cmd.ExecuteReader();

        //     long count = 0;
        //     while (reader.Read()) count++;
        //     return count;
        // }

        // [Benchmark(Description = "LeichtFrame GroupBy Count (HighCard)")]
        // public DataFrame LF_Group_Count_High()
        // {
        //     return _lfFrame.GroupBy("UniqueId").Count();
        // }

        // =========================================================
        // SUM
        // =========================================================

        // [Benchmark(Description = "DuckDB GroupBy Sum")]
        // public double DuckDB_Group_Sum()
        // {
        //     using var cmd = _duckConnection.CreateCommand();
        //     cmd.CommandText = "SELECT Category, SUM(Val) FROM BenchData GROUP BY Category";
        //     using var reader = cmd.ExecuteReader();

        //     double sumCheck = 0;
        //     while (reader.Read()) sumCheck += reader.GetDouble(1);
        //     return sumCheck;
        // }

        // [Benchmark(Description = "LeichtFrame GroupBy Sum")]
        // public DataFrame LF_Group_Sum()
        // {
        //     return _lfFrame.GroupBy("Category").Sum("Val");
        // }

        // =========================================================
        // MEAN
        // =========================================================

        // [Benchmark(Description = "DuckDB GroupBy Mean")]
        // public double DuckDB_Group_Mean()
        // {
        //     using var cmd = _duckConnection.CreateCommand();
        //     cmd.CommandText = "SELECT Category, AVG(Val) FROM BenchData GROUP BY Category";
        //     using var reader = cmd.ExecuteReader();

        //     double check = 0;
        //     while (reader.Read()) check += reader.GetDouble(1);
        //     return check;
        // }

        // [Benchmark(Description = "LeichtFrame GroupBy Mean")]
        // public DataFrame LF_Group_Mean()
        // {
        //     return _lfFrame.GroupBy("Category").Mean("Val");
        // }

        // =========================================================
        // MIN
        // =========================================================

        // [Benchmark(Description = "DuckDB GroupBy Min")]
        // public double DuckDB_Group_Min()
        // {
        //     using var cmd = _duckConnection.CreateCommand();
        //     cmd.CommandText = "SELECT Category, MIN(Val) FROM BenchData GROUP BY Category";
        //     using var reader = cmd.ExecuteReader();

        //     double check = 0;
        //     while (reader.Read()) check += reader.GetDouble(1);
        //     return check;
        // }

        // [Benchmark(Description = "LeichtFrame GroupBy Min")]
        // public DataFrame LF_Group_Min()
        // {
        //     return _lfFrame.GroupBy("Category").Min("Val");
        // }

        // =========================================================
        // MAX
        // =========================================================

        // [Benchmark(Description = "DuckDB GroupBy Max")]
        // public double DuckDB_Group_Max()
        // {
        //     using var cmd = _duckConnection.CreateCommand();
        //     cmd.CommandText = "SELECT Category, MAX(Val) FROM BenchData GROUP BY Category";
        //     using var reader = cmd.ExecuteReader();

        //     double check = 0;
        //     while (reader.Read()) check += reader.GetDouble(1);
        //     return check;
        // }

        // [Benchmark(Description = "LeichtFrame GroupBy Max")]
        // public DataFrame LF_Group_Max()
        // {
        //     return _lfFrame.GroupBy("Category").Max("Val");
        // }

        // =========================================================
        // STRING GROUPBY
        // =========================================================

        // [Benchmark(Description = "DuckDB GroupBy String")]
        // public long DuckDB_String()
        // {
        //     using var cmd = _duckConnection.CreateCommand();
        //     cmd.CommandText = "SELECT UniqueId, COUNT(*) FROM BenchData GROUP BY UniqueId";
        //     using var reader = cmd.ExecuteReader();

        //     long count = 0;
        //     while (reader.Read()) count++;
        //     return count;
        // }

        // [Benchmark(Description = "LeichtFrame GroupBy String")]
        // public DataFrame LF_String()
        // {
        //     return _lfFrame.GroupBy("UniqueId").Count();
        // }
    }
}