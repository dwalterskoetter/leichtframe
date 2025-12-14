using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;

namespace LeichtFrame.Benchmarks
{
    public class JoinBenchmarks : BenchmarkData
    {
        private LeichtFrame.Core.DataFrame _lfRight = null!;

        [GlobalSetup]
        public override void GlobalSetup()
        {
            base.GlobalSetup();

            // ---------------------------------------------------------
            // SETUP LEICHTFRAME RIGHT SIDE
            // ---------------------------------------------------------

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
            _lfRight?.Dispose();
            base.GlobalCleanup();
        }

        // --- BENCHMARKS ---

        [Benchmark(Baseline = true, Description = "DuckDB Join (Count)")]
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