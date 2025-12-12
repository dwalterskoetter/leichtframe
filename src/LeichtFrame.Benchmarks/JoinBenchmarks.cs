using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;
using MDA = Microsoft.Data.Analysis;

namespace LeichtFrame.Benchmarks
{
    public class JoinBenchmarks : BenchmarkData
    {
        private LeichtFrame.Core.DataFrame _lfRight = null!;
        private MDA.DataFrame _msRight = null!;
        private List<TestPoco> _linqRight = null!;

        [GlobalSetup]
        public override void GlobalSetup()
        {
            base.GlobalSetup();

            // ---------------------------------------------------------
            // SETUP RIGHT SIDE (C# Objects & LeichtFrame & MS)
            // ---------------------------------------------------------
            _linqRight = new List<TestPoco>(_pocoList);

            // LeichtFrame Schema
            var schemaRight = new DataFrameSchema(new[] {
                new ColumnDefinition("UniqueId", typeof(string)),
                new ColumnDefinition("RightVal", typeof(double))
            });

            _lfRight = DataFrame.Create(schemaRight, N);
            var colKey = (StringColumn)_lfRight["UniqueId"];
            var colVal = (DoubleColumn)_lfRight["RightVal"];

            for (int i = 0; i < N; i++)
            {
                colKey.Append(_pocoList[i].UniqueId);
                colVal.Append(_pocoList[i].Val * 2);
            }

            // MS DataFrame
            var tempIds = new string[N];
            var tempVals = new double[N];
            for (int i = 0; i < N; i++)
            {
                tempIds[i] = _pocoList[i].UniqueId;
                tempVals[i] = _pocoList[i].Val * 2;
            }
            var msKey = new MDA.StringDataFrameColumn("UniqueId", tempIds);
            var msVal = new MDA.PrimitiveDataFrameColumn<double>("RightVal", tempVals);
            _msRight = new MDA.DataFrame(msKey, msVal);

            // ---------------------------------------------------------
            // SETUP DUCKDB RIGHT SIDE 🦆
            // ---------------------------------------------------------
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "CREATE TABLE BenchDataRight (UniqueId VARCHAR, RightVal DOUBLE)";
            cmd.ExecuteNonQuery();

            using (var appender = _duckConnection.CreateAppender("BenchDataRight"))
            {
                for (int i = 0; i < N; i++)
                {
                    var row = appender.CreateRow();
                    row.AppendValue(_pocoList[i].UniqueId);
                    row.AppendValue(_pocoList[i].Val * 2);
                    row.EndRow();
                }
            }
        }

        [GlobalCleanup]
        public override void GlobalCleanup()
        {
            base.GlobalCleanup();
            _lfRight?.Dispose();
        }

        // --- BENCHMARKS ---

        [Benchmark(Baseline = true, Description = "LINQ Join")]
        [WarmupCount(1)]
        [IterationCount(3)]
        public object Linq_Join()
        {
            return _pocoList.Join(
                _linqRight,
                left => left.UniqueId,
                right => right.UniqueId,
                (left, right) => new { L = left.Val, R = right.Val }
            ).Count();
        }

        [Benchmark(Description = "MS DataFrame Merge")]
        [WarmupCount(1)]
        [IterationCount(3)]
        public object MS_Join()
        {
            return _msFrame.Merge(_msRight, new[] { "UniqueId" }, new[] { "UniqueId" });
        }

        [Benchmark(Description = "DuckDB Join")]
        public long DuckDB_Join()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = @"
                SELECT COUNT(*) 
                FROM BenchData 
                INNER JOIN BenchDataRight ON BenchData.UniqueId = BenchDataRight.UniqueId";

            return (long)cmd.ExecuteScalar()!;
        }

        [Benchmark(Description = "LeichtFrame Join")]
        public DataFrame LF_Join()
        {
            return _lfFrame.Join(_lfRight, "UniqueId", JoinType.Inner);
        }
    }
}