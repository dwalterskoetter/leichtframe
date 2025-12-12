using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;

namespace LeichtFrame.Benchmarks
{
    public class GroupByBenchmarks : BenchmarkData
    {
        // --- Low Cardinality ---

        [Benchmark(Baseline = true, Description = "LINQ GroupBy (LowCard)")]
        public object Linq_Group_Low()
        {
            return _pocoList.GroupBy(x => x.Category)
                            .ToDictionary(g => g.Key, g => g.Count());
        }

        [Benchmark(Description = "MS DataFrame GroupBy (LowCard)")]
        public object MS_Group_Low()
        {
            return _msFrame.GroupBy("Category").Count();
        }

        [Benchmark(Description = "DuckDB GroupBy (LowCard)")]
        public object DuckDB_Group_Low()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Category, COUNT(*) FROM BenchData GROUP BY Category";
            using var reader = cmd.ExecuteReader();

            int count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LeichtFrame GroupBy (LowCard)")]
        public DataFrame LF_Group_Low()
        {
            return _lfFrame.GroupBy("Category").Count();
        }

        // --- High Cardinality ---

        [Benchmark(Description = "LINQ GroupBy (HighCard)")]
        [WarmupCount(1)]
        [IterationCount(3)]
        public object Linq_Group_High()
        {
            return _pocoList.GroupBy(x => x.UniqueId)
                            .ToDictionary(g => g.Key, g => g.Sum(i => i.Id));
        }

        [Benchmark(Description = "MS DataFrame GroupBy (HighCard)")]
        [WarmupCount(1)]
        [IterationCount(3)]
        public object MS_Group_High()
        {
            try
            {
                return _msFrame.GroupBy("UniqueId").Sum("Id");
            }
            catch (Exception)
            {
                return null!;
            }
        }

        [Benchmark(Description = "DuckDB GroupBy (HighCard)")]
        public object DuckDB_Group_High()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT UniqueId, SUM(Id) FROM BenchData GROUP BY UniqueId";
            using var reader = cmd.ExecuteReader();

            int count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LeichtFrame GroupBy (HighCard)")]
        public DataFrame LF_Group_High()
        {
            return _lfFrame.GroupBy("UniqueId").Sum("Id");
        }
    }
}