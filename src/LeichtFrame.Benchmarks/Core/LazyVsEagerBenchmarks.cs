using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;
using LeichtFrame.Core.Operations.Transform;
using LeichtFrame.Core.Operations.Filter;
using static LeichtFrame.Core.Expressions.F;

namespace LeichtFrame.Benchmarks
{
    public class LazyVsEagerBenchmarks : BenchmarkData
    {
        [Benchmark(Baseline = true, Description = "DuckDB (SQL)")]
        public long DuckDB_Query()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT (Val * 2.0) FROM BenchData WHERE Val > 500.0";
            using var reader = cmd.ExecuteReader();

            long count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LF Eager (Delegate)")]
        public DataFrame LF_Eager_Delegate()
        {
            var filtered = _lfFrame.Where(r => r.Get<double>("Val") > 500.0);

            var result = filtered.AddColumn("Result", r => r.Get<double>("Val") * 2.0);

            return result.Select("Result");
        }

        [Benchmark(Description = "LF Lazy (Expression)")]
        public DataFrame LF_Lazy_Expr()
        {
            return _lfFrame.Lazy()
                .Where(Col("Val") > 500.0)
                .Select(
                    (Col("Val") * 2.0).As("Result")
                )
                .Collect();
        }

        [Benchmark(Description = "LF Manual (Hardcoded SIMD)")]
        public DataFrame LF_Manual_Hardcoded()
        {
            var filtered = _lfFrame.WhereVec("Val", CompareOp.GreaterThan, 500.0);

            var srcCol = (DoubleColumn)filtered["Val"];
            var calcCol = srcCol * 2.0;

            var finalCol = new DoubleColumn("Result", calcCol.Length, calcCol.IsNullable);
            for (int i = 0; i < calcCol.Length; i++)
            {
                if (calcCol.IsNull(i)) finalCol.Append(null);
                else finalCol.Append(calcCol.Get(i));
            }

            return new DataFrame(new[] { finalCol });
        }
    }
}