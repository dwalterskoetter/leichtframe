using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;
using LeichtFrame.IO;

namespace LeichtFrame.Benchmarks
{
    [MemoryDiagnoser]
    public class CsvBenchmarks
    {
        private string _csvPath = "";
        private DataFrameSchema _schema = null!;

        [GlobalSetup]
        public void Setup()
        {
            // 1. Generate a "large" CSV file (approx 10MB - 100k rows)
            _csvPath = Path.Combine(Path.GetTempPath(), "benchmark_data.csv");

            using var writer = new StreamWriter(_csvPath);
            writer.WriteLine("Id,Value,Category,Description"); // Header

            var rnd = new Random(42);

            for (int i = 0; i < 100_000; i++)
            {
                int id = i;
                double val = rnd.NextDouble() * 10000;
                int cat = rnd.Next(0, 5);
                writer.WriteLine($"{id},{val:F4},Category_{cat},Some description text for row {id}");
            }

            // 2. Define Schema manually
            _schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Value", typeof(double)),
                new ColumnDefinition("Category", typeof(string)),
                new ColumnDefinition("Description", typeof(string))
            });
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            if (File.Exists(_csvPath)) File.Delete(_csvPath);
        }

        [Benchmark(Description = "CSV Read (Parallel Optimized)")]
        public DataFrame ReadFile()
        {
            return CsvReader.Read(_csvPath, _schema);
        }
    }
}