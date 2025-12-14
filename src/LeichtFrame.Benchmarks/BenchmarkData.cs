using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using LeichtFrame.Core;
using DuckDB.NET.Data;

namespace LeichtFrame.Benchmarks
{
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [RankColumn]
    public abstract class BenchmarkData
    {
        [Params(1_000_000)]
        public int N;

        protected LeichtFrame.Core.DataFrame _lfFrame = null!;
        protected DuckDBConnection _duckConnection = null!;

        protected List<TestPoco> _pocoList = null!;

        public record TestPoco(int Id, double Val, string Category, string UniqueId);

        [GlobalSetup]
        public virtual void GlobalSetup()
        {
            var rnd = new Random(42);
            _pocoList = new List<TestPoco>(N);
            var categories = new[] { "A", "B", "C", "D", "E" };

            for (int i = 0; i < N; i++)
            {
                int id = rnd.Next(0, 100_000);
                double val = rnd.NextDouble() * 1000.0;
                string cat = categories[rnd.Next(categories.Length)];
                string uid = Guid.NewGuid().ToString();

                _pocoList.Add(new TestPoco(id, val, cat, uid));
            }

            // --- 1. Setup LeichtFrame ---
            _lfFrame = DataFrame.FromObjects(_pocoList);

            // --- 2. Setup DuckDB 🦆 ---
            _duckConnection = new DuckDBConnection("DataSource=:memory:");
            _duckConnection.Open();

            using var cmd = _duckConnection.CreateCommand();

            cmd.CommandText = "CREATE TABLE BenchData (Id INTEGER, Val DOUBLE, Category VARCHAR, UniqueId VARCHAR)";
            cmd.ExecuteNonQuery();

            using (var appender = _duckConnection.CreateAppender("BenchData"))
            {
                foreach (var item in _pocoList)
                {
                    var row = appender.CreateRow();
                    row.AppendValue(item.Id);
                    row.AppendValue(item.Val);
                    row.AppendValue(item.Category);
                    row.AppendValue(item.UniqueId);
                    row.EndRow();
                }
            }
        }

        [GlobalCleanup]
        public virtual void GlobalCleanup()
        {
            _lfFrame?.Dispose();
            _duckConnection?.Dispose();
        }
    }
}