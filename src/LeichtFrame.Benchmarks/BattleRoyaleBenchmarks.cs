using BenchmarkDotNet.Attributes;

// Your Library
using LeichtFrame.Core;

// The Competitor (Alias 'MDA')
using MDA = Microsoft.Data.Analysis;

namespace LeichtFrame.Benchmarks
{
    [MemoryDiagnoser]
    public class BattleRoyaleBenchmarks
    {
        [Params(1_000_000)]
        public int N;

        // 1. LINQ
        private List<int> _linqList = null!;

        // 2. LeichtFrame
        private LeichtFrame.Core.DataFrame _lfFrame = null!;

        // 3. Microsoft Data Analysis
        private MDA.DataFrame _mdaFrame = null!;

        [GlobalSetup]
        public void Setup()
        {
            // --- Generate Data ---
            var data = new int[N];
            var rnd = new Random(42);
            for (int i = 0; i < N; i++) data[i] = rnd.Next(100);

            // 1. Setup LINQ
            _linqList = new List<int>(data);

            // 2. Setup LeichtFrame
            var schema = new DataFrameSchema(new[] { new ColumnDefinition("Val", typeof(int)) });
            _lfFrame = LeichtFrame.Core.DataFrame.Create(schema, N);
            var lfCol = (IntColumn)_lfFrame["Val"];
            foreach (var val in data) lfCol.Append(val);

            // 3. Setup Microsoft.Data.Analysis
            // MDA uses "PrimitiveDataFrameColumn" for int
            var mdaCol = new MDA.PrimitiveDataFrameColumn<int>("Val", N);
            for (int i = 0; i < N; i++) mdaCol[i] = data[i];
            _mdaFrame = new MDA.DataFrame(mdaCol);
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _lfFrame.Dispose();
            // MDA has no Dispose for DataFrame, relies on GC
        }

        // --- ROUND 1: Calculation Speed (Sum) ---

        [Benchmark(Baseline = true)]
        public long Calc_LINQ()
        {
            return _linqList.Sum(x => (long)x);
        }

        [Benchmark]
        public double Calc_LeichtFrame()
        {
            return _lfFrame.Sum("Val");
        }

        [Benchmark]
        public double Calc_Microsoft()
        {
            // MDA API is similar, but sometimes uses dynamic calls internally in Sum()
            // Since MDA.DataFrame["Val"] returns an abstract Column,
            // we might need to cast to PrimitiveDataFrameColumn for speed?
            // We use the standard API.
            return (double)_mdaFrame["Val"].Sum();
        }

        // --- ROUND 2: Memory Efficiency (Creation) ---
        // Note: We measure creation including data population

        [Benchmark]
        public List<int> Create_LINQ()
        {
            var list = new List<int>(N);
            for (int i = 0; i < N; i++) list.Add(i);
            return list;
        }

        [Benchmark]
        public LeichtFrame.Core.DataFrame Create_LeichtFrame()
        {
            var schema = new DataFrameSchema(new[] { new ColumnDefinition("Val", typeof(int)) });
            var df = LeichtFrame.Core.DataFrame.Create(schema, N);
            var col = (IntColumn)df["Val"];
            for (int i = 0; i < N; i++) col.Append(i);
            return df;
        }

        [Benchmark]
        public MDA.DataFrame Create_Microsoft()
        {
            var col = new MDA.PrimitiveDataFrameColumn<int>("Val", N);
            for (int i = 0; i < N; i++) col[i] = i;
            return new MDA.DataFrame(col);
        }
    }
}