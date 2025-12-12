using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;
using MDA = Microsoft.Data.Analysis;

namespace LeichtFrame.Benchmarks
{
    public class FilterBenchmarks : BenchmarkData
    {
        // Filter: Id < 50_000

        [Benchmark(Baseline = true, Description = "LINQ Where (ToList)")]
        [WarmupCount(1)]
        [IterationCount(5)]
        public List<TestPoco> Linq_Where()
        {
            return _pocoList.Where(x => x.Id < 50_000).ToList();
        }

        [Benchmark(Description = "MS DataFrame Filter")]
        [WarmupCount(1)]
        [IterationCount(5)]
        public MDA.DataFrame MS_Filter()
        {
            var col = _msFrame.Columns["Id"];
            var filterMask = col.ElementwiseLessThan(50_000);
            return _msFrame.Filter(filterMask);
        }

        [Benchmark(Description = "DuckDB Filter")]
        public object DuckDB_Filter()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT * FROM BenchData WHERE Id < 50000";
            using var reader = cmd.ExecuteReader();

            int count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LeichtFrame Where")]
        public DataFrame LF_Where()
        {
            return _lfFrame.Where(row => row.Get<int>("Id") < 50_000);
        }

        [Benchmark(Description = "LeichtFrame Where (SIMD)")]
        public DataFrame LF_WhereVec()
        {
            return _lfFrame.WhereVec("Id", CompareOp.LessThan, 50_000);
        }
    }
}