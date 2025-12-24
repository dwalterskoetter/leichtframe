using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using DuckDB.NET.Data;
using LeichtFrame.Core;
using LeichtFrame.Core.Engine.Algorithms.Converter; // Zugriff auf Internals nötig
using LeichtFrame.Core.Expressions;

namespace LeichtFrame.Benchmarks.Breakdown
{
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    public class LowCardBreakdownBenchmarks
    {
        [Params(1_000_000)]
        public int N;

        private DataFrame _stringDf = null!;
        private DataFrame _categoryDf = null!;
        private DuckDBConnection _duckConnection = null!;

        [GlobalSetup]
        public void Setup()
        {
            // 1. Daten generieren (5 Kategorien)
            var cats = new[] { "Electronics", "Books", "Garden", "Auto", "Food" };
            var rnd = new Random(42);

            var colCat = new StringColumn("Category", N);
            var colVal = new DoubleColumn("Val", N);

            // Für DuckDB
            _duckConnection = new DuckDBConnection("DataSource=:memory:");
            _duckConnection.Open();
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "CREATE TABLE BenchData (Category VARCHAR, Val DOUBLE)";
            cmd.ExecuteNonQuery();

            using var appender = _duckConnection.CreateAppender("BenchData");

            for (int i = 0; i < N; i++)
            {
                string c = cats[rnd.Next(cats.Length)];
                double v = rnd.NextDouble();

                colCat.Append(c);
                colVal.Append(v);

                appender.CreateRow().AppendValue(c).AppendValue(v).EndRow();
            }

            // 2. Normaler DataFrame (String)
            _stringDf = new DataFrame(new IColumn[] { colCat, colVal });

            // 3. Pre-Converted DataFrame (Category)
            // Wir simulieren hier den Zustand NACH der internen Optimierung
            // oder wenn der User das Schema beim Laden schon definiert hätte.
            var catCol = StringConverter.ToCategory(colCat); // Manueller Aufruf des internen Converters
            var valColClone = (DoubleColumn)colVal.CloneSubset(Enumerable.Range(0, N).ToArray()); // Clone für Fairness

            _categoryDf = new DataFrame(new IColumn[] { catCol, valColClone });
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _stringDf?.Dispose();
            _categoryDf?.Dispose();
            _duckConnection?.Dispose();
        }

        // =========================================================
        // 1. BASELINE: DUCKDB
        // =========================================================

        [Benchmark(Description = "1. DuckDB (Total)")]
        public double DuckDB_Total()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Category, SUM(Val) FROM BenchData GROUP BY Category";
            // Wir nutzen ExecuteScalar um Overhead zu minimieren und Engine zu messen
            // (Simuliert "Stream" Ansatz grob)
            using var reader = cmd.ExecuteReader();
            double sum = 0;
            while (reader.Read()) sum += reader.GetDouble(1);
            return sum;
        }

        // =========================================================
        // 2. LEICHTFRAME: FULL AUTO (User Experience)
        // =========================================================

        [Benchmark(Description = "2. LF Auto (Total)")]
        public DataFrame LF_Auto_Total()
        {
            // Das ist der aktuelle langsame Pfad:
            // 1. Check LowCard
            // 2. Convert to Category (Hashing)
            // 3. GroupBy Ints
            return _stringDf.Lazy()
                            .GroupBy("Category")
                            .Agg("Val".Sum())
                            .Collect();
        }

        // =========================================================
        // 3. LEICHTFRAME: NUR KONVERTIERUNG (The Bottleneck)
        // =========================================================

        [Benchmark(Description = "3. LF Step: Conversion")]
        public void LF_Step_Conversion()
        {
            var strCol = (StringColumn)_stringDf["Category"];
            // Wir messen NUR wie lange es dauert, aus Strings Ints zu machen.
            // Das ist der Teil, den wir optimiert haben (Unsafe Converter).
            using var catCol = StringConverter.ToCategory(strCol);
        }

        // =========================================================
        // 4. LEICHTFRAME: NUR GROUPING (The Engine Speed)
        // =========================================================

        [Benchmark(Description = "4. LF Step: Grouping (Pre-Converted)")]
        public DataFrame LF_Step_Grouping()
        {
            // Hier arbeiten wir auf dem _categoryDf.
            // Die Spalte ist bereits 'CategoryColumn'.
            // Die Engine springt SOFORT in den 'DirectAddressingStrategy' (Histogramm).
            return _categoryDf.Lazy()
                              .GroupBy("Category")
                              .Agg("Val".Sum())
                              .Collect();
        }
    }
}