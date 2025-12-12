using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using LeichtFrame.Core;
using MDA = Microsoft.Data.Analysis;
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
        protected MDA.DataFrame _msFrame = null!;
        protected List<TestPoco> _pocoList = null!;
        protected DuckDBConnection _duckConnection = null!;
        public record TestPoco(int Id, double Val, string Category, string UniqueId);

        [GlobalSetup]
        public virtual void GlobalSetup()
        {
            var rnd = new Random(42);

            _pocoList = new List<TestPoco>(N);
            var categories = new[] { "A", "B", "C", "D", "E" };

            var dataInt = new int[N];
            var dataDbl = new double[N];
            var dataCat = new string[N];
            var dataUid = new string[N];

            for (int i = 0; i < N; i++)
            {
                int id = rnd.Next(0, 100_000);
                double val = rnd.NextDouble() * 1000.0;
                string cat = categories[rnd.Next(categories.Length)];
                string uid = Guid.NewGuid().ToString();

                _pocoList.Add(new TestPoco(id, val, cat, uid));

                dataInt[i] = id; dataDbl[i] = val; dataCat[i] = cat; dataUid[i] = uid;
            }

            // --- 1. Setup LeichtFrame ---
            _lfFrame = DataFrame.FromObjects(_pocoList);

            // --- 2. Setup Microsoft.Data.Analysis ---
            var msInt = new MDA.PrimitiveDataFrameColumn<int>("Id", dataInt);
            var msDbl = new MDA.PrimitiveDataFrameColumn<double>("Val", dataDbl);
            var msCat = new MDA.StringDataFrameColumn("Category", dataCat);
            var msId = new MDA.StringDataFrameColumn("UniqueId", dataUid);

            _msFrame = new MDA.DataFrame(msInt, msDbl, msCat, msId);

            // --- 3. Setup DuckDB 🦆 ---
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