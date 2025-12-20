using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;
using LeichtFrame.Core.Operations.Join;

namespace LeichtFrame.Benchmarks
{
    public class JoinBenchmarks : BenchmarkData
    {
        private LeichtFrame.Core.DataFrame _lfRight = null!;

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

            // We purposefully create a smaller right dataset (50% of Left)
            // to ensure Left Join produces Nulls and Inner Join filters rows.
            int rightCount = N / 2;
            _lfRight = DataFrame.Create(schemaRight, rightCount);

            var colKey = (StringColumn)_lfRight["UniqueId"];
            var colVal = (DoubleColumn)_lfRight["RightVal"];

            // Insert every 2nd item -> 50% match rate
            for (int i = 0; i < N; i += 2)
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
                for (int i = 0; i < N; i += 2)
                {
                    var row = appender.CreateRow();
                    row.AppendValue(_pocoList[i].UniqueId);
                    row.AppendValue(_pocoList[i].Val * 2);
                    row.EndRow();
                }
            }
        }

        public override void GlobalCleanup()
        {
            _lfRight?.Dispose();
            base.GlobalCleanup();
        }

        // =========================================================
        // INNER JOIN
        // =========================================================

        [Benchmark(Baseline = true, Description = "DuckDB Inner Join")]
        public long DuckDB_InnerJoin()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = @"
                SELECT COUNT(*) 
                FROM BenchData 
                INNER JOIN BenchDataRight ON BenchData.UniqueId = BenchDataRight.UniqueId";

            return (long)cmd.ExecuteScalar()!;
        }

        [Benchmark(Description = "LeichtFrame Inner Join")]
        public DataFrame LF_InnerJoin()
        {
            return _lfFrame.Join(_lfRight, "UniqueId", JoinType.Inner);
        }

        // =========================================================
        // LEFT JOIN
        // =========================================================

        [Benchmark(Description = "DuckDB Left Join")]
        public long DuckDB_LeftJoin()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = @"
                SELECT COUNT(*) 
                FROM BenchData 
                LEFT JOIN BenchDataRight ON BenchData.UniqueId = BenchDataRight.UniqueId";

            return (long)cmd.ExecuteScalar()!;
        }

        [Benchmark(Description = "LeichtFrame Left Join")]
        public DataFrame LF_LeftJoin()
        {
            return _lfFrame.Join(_lfRight, "UniqueId", JoinType.Left);
        }
    }
}