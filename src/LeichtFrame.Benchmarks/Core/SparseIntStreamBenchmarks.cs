using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using LeichtFrame.Core;
using LeichtFrame.Core.Engine.Kernels.GroupBy.Strategies; // Für direkten Strategie-Test
using LeichtFrame.Core.Operations.Aggregate; // Für CountStream
using LeichtFrame.Core.Operations.GroupBy;   // Für GroupBy
using LeichtFrame.Core.Expressions;          // Für F.Col()

namespace LeichtFrame.Benchmarks.Breakdown
{
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    public class SparseIntStreamingBreakdownBenchmarks
    {
        [Params(1_000_000)]
        public int N;

        private DataFrame _intDf = null!;
        private GroupedDataFrame _preparedGdf = null!;

        [GlobalSetup]
        public void Setup()
        {
            // Sparse Data: Wertebereich 0..2.000.000 bei 1.000.000 Zeilen.
            // Das erzwingt IntSwissMapStrategy (da Range > 1M).
            // Es gibt Kollisionen (Gruppen), aber es ist sparse.
            var rnd = new Random(42);
            var colVal = new IntColumn("Val", N);

            for (int i = 0; i < N; i++)
            {
                colVal.Append(rnd.Next(0, 2_000_000));
            }

            _intDf = new DataFrame(new IColumn[] { colVal });

            // Prepare GDF for Step 3 (Simulation)
            // Wir führen das einmal aus, um ein "fertiges" GDF für die Iterations-Messung zu haben.
            // WICHTIG: Das nutzt IntSwissMapStrategy intern.
            _preparedGdf = _intDf.GroupBy("Val");
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _intDf?.Dispose();
            _preparedGdf?.Dispose();
        }

        // =========================================================
        // STEP 1: GROUPING ONLY (IntSwissMapStrategy)
        // =========================================================
        // Misst: NativeIntMap Aufbau + CSR Erstellung.
        // Erwartung: Minimal Managed Alloc (nur die Wrapper-Objekte).
        // Wenn hier 4 MB sind -> Die Strategie nutzt ein int[] Array intern.

        [Benchmark(Description = "1. Grouping Phase (SwissMap)")]
        public int Step1_GroupBy()
        {
            using var gdf = _intDf.GroupBy("Val");
            return gdf.GroupCount;
        }

        // =========================================================
        // STEP 2: STREAM CREATION (No Iteration)
        // =========================================================
        // Misst: Lazy Plan + Optimizer Overhead.

        [Benchmark(Description = "2. Plan Stream")]
        public object Step2_CreateStream()
        {
            return _intDf.Lazy()
                         .GroupBy("Val")
                         .Agg(F.Count().As("Count"))
                         .CollectStream();
        }

        // =========================================================
        // STEP 3: ITERATION ONLY (Native Enumerator)
        // =========================================================
        // Misst: Das Durchlaufen der Pointer.
        // Erwartung: 0 Alloc.

        [Benchmark(Description = "3. Iteration Phase")]
        public int Step3_Iterate()
        {
            int sum = 0;
            // Wir nutzen CountStream, da dies den NativeGroupCountEnumerator nutzt.
            // Das ist der Kern des FastNativeEnumerators (nur ohne RowView-Overhead).
            foreach (var (key, count) in _preparedGdf.CountStream())
            {
                sum += count;
            }
            return sum;
        }

        // =========================================================
        // TOTAL: Full Pipeline (Der Problemfall)
        // =========================================================

        [Benchmark(Description = "4. Total Pipeline (CollectStream)")]
        public int Total()
        {
            // Das ist das Szenario aus dem Haupt-Benchmark, wo 3.9 MB auftauchen.
            var stream = _intDf.Lazy()
                               .GroupBy("Val")
                               .Agg(F.Count().As("Count"))
                               .CollectStream();

            int sum = 0;
            foreach (var row in stream)
            {
                sum += row.Get<int>("Count");
            }
            return sum;
        }
    }
}