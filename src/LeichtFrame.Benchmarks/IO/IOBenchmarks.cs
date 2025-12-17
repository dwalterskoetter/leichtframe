using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using DuckDB.NET.Data;
using LeichtFrame.Core;
using LeichtFrame.IO;

namespace LeichtFrame.Benchmarks
{
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    [RankColumn]
    public class IOBenchmarks
    {
        [Params(100_000)]
        public int N;

        private string _tempDir = null!;
        private string _sourceCsvPath = null!;
        private string _sourceParquetPath = null!;
        private string _destCsvPath = null!;
        private string _destParquetPath = null!;

        private DataFrame _lfDataFrame = null!;
        private DuckDBConnection _duckConnection = null!;
        private DataFrameSchema _schema = null!;

        [GlobalSetup]
        public void Setup()
        {
            // 1. Prepare Temp Directory
            _tempDir = Path.Combine(Path.GetTempPath(), "LeichtFrame_IO_Bench");
            if (!Directory.Exists(_tempDir)) Directory.CreateDirectory(_tempDir);

            _sourceCsvPath = Path.Combine(_tempDir, "source.csv");
            _sourceParquetPath = Path.Combine(_tempDir, "source.parquet");
            _destCsvPath = Path.Combine(_tempDir, "dest.csv");
            _destParquetPath = Path.Combine(_tempDir, "dest.parquet");

            // 2. Generate Test Data
            var data = new List<BenchmarkData.TestPoco>(N);
            var rnd = new Random(42);
            var categories = new[] { "A", "B", "C", "D", "E" };

            for (int i = 0; i < N; i++)
            {
                data.Add(new BenchmarkData.TestPoco(
                    i,
                    rnd.NextDouble() * 1000.0,
                    categories[rnd.Next(categories.Length)],
                    Guid.NewGuid().ToString()
                ));
            }

            _lfDataFrame = DataFrame.FromObjects(data);
            _schema = _lfDataFrame.Schema;

            // 3. Write Source Files on disk
            _lfDataFrame.WriteCsv(_sourceCsvPath, new CsvWriteOptions { WriteHeader = true });
            _lfDataFrame.WriteParquet(_sourceParquetPath);

            // 4. DuckDB Setup
            _duckConnection = new DuckDBConnection("DataSource=:memory:");
            _duckConnection.Open();

            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "CREATE TABLE BenchData (Id INTEGER, Val DOUBLE, Category VARCHAR, UniqueId VARCHAR)";
            cmd.ExecuteNonQuery();

            using (var appender = _duckConnection.CreateAppender("BenchData"))
            {
                foreach (var item in data)
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
        public void Cleanup()
        {
            _lfDataFrame?.Dispose();
            _duckConnection?.Dispose();

            if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, true);
        }

        // =========================================================
        // CSV READ
        // =========================================================

        [Benchmark(Description = "DuckDB Read CSV")]
        public long DuckDB_Read_CSV()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = $"SELECT * FROM read_csv_auto('{_sourceCsvPath}')";
            using var reader = cmd.ExecuteReader();

            long count = 0;
            while (reader.Read())
            {
                count++;
                var id = reader.GetValue(0);
            }
            return count;
        }

        [Benchmark(Description = "LeichtFrame Read CSV")]
        public DataFrame LF_Read_CSV()
        {
            return CsvReader.Read(_sourceCsvPath, _schema, new CsvReadOptions { HasHeader = true });
        }

        // =========================================================
        // PARQUET READ
        // =========================================================

        [Benchmark(Description = "DuckDB Read Parquet")]
        public long DuckDB_Read_Parquet()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = $"SELECT * FROM read_parquet('{_sourceParquetPath}')";
            using var reader = cmd.ExecuteReader();

            long count = 0;
            while (reader.Read())
            {
                count++;
                var id = reader.GetValue(0);
            }
            return count;
        }

        [Benchmark(Description = "LeichtFrame Read Parquet")]
        public DataFrame LF_Read_Parquet()
        {
            // Nutzt intern Parquet.Net
            return ParquetReader.Read(_sourceParquetPath);
        }

        // =========================================================
        // CSV WRITE
        // =========================================================

        [Benchmark(Description = "DuckDB Write CSV")]
        public void DuckDB_Write_CSV()
        {
            using var cmd = _duckConnection.CreateCommand();
            // COPY (SELECT ...) TO ...
            cmd.CommandText = $"COPY BenchData TO '{_destCsvPath}' (FORMAT CSV, HEADER)";
            cmd.ExecuteNonQuery();
        }

        [Benchmark(Description = "LeichtFrame Write CSV")]
        public void LF_Write_CSV()
        {
            _lfDataFrame.WriteCsv(_destCsvPath);
        }

        // =========================================================
        // PARQUET WRITE
        // =========================================================

        [Benchmark(Description = "DuckDB Write Parquet")]
        public void DuckDB_Write_Parquet()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = $"COPY BenchData TO '{_destParquetPath}' (FORMAT PARQUET)";
            cmd.ExecuteNonQuery();
        }

        [Benchmark(Description = "LeichtFrame Write Parquet")]
        public void LF_Write_Parquet()
        {
            _lfDataFrame.WriteParquet(_destParquetPath);
        }
    }
}