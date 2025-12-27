using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using DuckDB.NET.Data;
using LeichtFrame.Core;

namespace LeichtFrame.Benchmarks
{
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [RankColumn]
    public class GrpImproveBenchmarks
    {
        [Params(1_000_000)]
        public int N;

        private DataFrame _lfFrame = null!;
        private DuckDBConnection _duckConnection = null!;

        [GlobalSetup]
        public void Setup()
        {
            Console.WriteLine($"Generating {N} rows of test data...");

            var ids = new int[N];
            var zipCodes = new int[N];
            var categories = new string[N];
            var uuids = new string[N];
            var values = new double[N];

            var rnd = new Random(42);
            var cats = new[] { "Electronics", "Books", "Garden", "Auto", "Food", "Music", "Toys", "Tools", "Pets", "Sports" };

            var cId = new IntColumn("Id", N);
            var cZip = new IntColumn("Zip", N);
            var cCat = new StringColumn("Category", N);
            var cUuid = new StringColumn("UUID", N);
            var cVal = new DoubleColumn("Val", N);

            for (int i = 0; i < N; i++)
            {
                int id = i * 2;
                int zip = rnd.Next(0, 1000);
                string cat = cats[rnd.Next(cats.Length)];
                string uid = Guid.NewGuid().ToString();
                double val = rnd.NextDouble() * 100.0;

                ids[i] = id;
                zipCodes[i] = zip;
                categories[i] = cat;
                uuids[i] = uid;
                values[i] = val;

                cId.Append(id);
                cZip.Append(zip);
                cCat.Append(cat);
                cUuid.Append(uid);
                cVal.Append(val);
            }

            _lfFrame = new DataFrame(new IColumn[] { cId, cZip, cCat, cUuid, cVal });

            _duckConnection = new DuckDBConnection("DataSource=:memory:");
            _duckConnection.Open();

            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "CREATE TABLE BenchData (Id INTEGER, Zip INTEGER, Category VARCHAR, UUID VARCHAR, Val DOUBLE)";
            cmd.ExecuteNonQuery();

            using (var appender = _duckConnection.CreateAppender("BenchData"))
            {
                for (int i = 0; i < N; i++)
                {
                    var row = appender.CreateRow();
                    row.AppendValue(ids[i]);
                    row.AppendValue(zipCodes[i]);
                    row.AppendValue(categories[i]);
                    row.AppendValue(uuids[i]);
                    row.AppendValue(values[i]);
                    row.EndRow();
                }
            }
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _lfFrame?.Dispose();
            _duckConnection?.Dispose();
        }

        // =========================================================
        // SCENARIO B: SPARSE INT (SwissMap vs Hash)
        // =========================================================

        [Benchmark(Description = "DuckDB: Sparse Int (Stream)")]
        public long DuckDB_B_Stream()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Id, COUNT(*) FROM BenchData GROUP BY Id";
            using var reader = cmd.ExecuteReader();

            long checkSum = 0;
            while (reader.Read())
            {
                int key = reader.GetInt32(0);
                long cnt = reader.GetInt64(1);
                checkSum += key + cnt;
            }
            return checkSum;
        }

        [Benchmark(Description = "LF: Sparse Int (Stream)")]
        public long LF_B_Stream()
        {
            var stream = _lfFrame.Lazy()
                                 .GroupBy("Id")
                                 .Agg("*".Count().As("Cnt"))
                                 .CollectStream();

            long checkSum = 0;
            foreach (var row in stream)
            {
                int key = row.Get<int>(0);
                int cnt = row.Get<int>(1);
                checkSum += key + cnt;
            }
            return checkSum;
        }

        [Benchmark(Description = "DuckDB: Sparse Int (Mat)")]
        public List<object[]> DuckDB_B_Mat()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Id, COUNT(*) FROM BenchData GROUP BY Id";
            using var reader = cmd.ExecuteReader();

            var result = new List<object[]>();
            while (reader.Read())
            {
                result.Add(new object[] { reader.GetValue(0), reader.GetValue(1) });
            }
            return result;
        }

        [Benchmark(Description = "LF: Sparse Int (Mat)")]
        public DataFrame LF_B_Mat()
        {
            return _lfFrame.Lazy()
                           .GroupBy("Id")
                           .Agg("*".Count().As("Cnt"))
                           .Collect();
        }


        // =========================================================
        // SCENARIO D: HIGH CARDINALITY STRING (Parallel / SwissMap)
        // =========================================================

        [Benchmark(Description = "DuckDB: HighCard String (Stream)")]
        public long DuckDB_D_Stream()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT UUID, COUNT(*) FROM BenchData GROUP BY UUID";
            using var reader = cmd.ExecuteReader();

            long checkSum = 0;
            while (reader.Read())
            {
                string uuid = reader.GetString(0);
                long cnt = reader.GetInt64(1);
                checkSum += cnt + uuid.Length;
            }
            return checkSum;
        }

        [Benchmark(Description = "LF: HighCard String (Stream)")]
        public long LF_D_Stream()
        {
            var stream = _lfFrame.Lazy()
                                 .GroupBy("UUID")
                                 .Agg("*".Count().As("Cnt"))
                                 .CollectStream();

            long checkSum = 0;
            foreach (var row in stream)
            {
                string uuid = row.Get<string>(0);
                int cnt = row.Get<int>(1);
                checkSum += cnt + uuid.Length;
            }
            return checkSum;
        }

        [Benchmark(Description = "DuckDB: HighCard String (Mat)")]
        public List<object[]> DuckDB_D_Mat()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT UUID, COUNT(*) FROM BenchData GROUP BY UUID";
            using var reader = cmd.ExecuteReader();

            var result = new List<object[]>();
            while (reader.Read())
            {
                result.Add(new object[] { reader.GetValue(0), reader.GetValue(1) });
            }
            return result;
        }

        [Benchmark(Description = "LF: HighCard String (Mat)")]
        public DataFrame LF_D_Mat()
        {
            return _lfFrame.Lazy()
                           .GroupBy("UUID")
                           .Agg("*".Count().As("Cnt"))
                           .Collect();
        }
    }
}