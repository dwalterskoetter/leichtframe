using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using LeichtFrame.Core;
using LeichtFrame.Core.Expressions;

namespace LeichtFrame.Benchmarks.Breakdown
{
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    public class MultiColumnStreamingBenchmarks
    {
        [Params(1_000_000)]
        public int N;

        private DataFrame _df = null!;

        [GlobalSetup]
        public void Setup()
        {
            var rnd = new Random(42);
            var colA = new IntColumn("A", N);
            var colB = new IntColumn("B", N);

            for (int i = 0; i < N; i++)
            {
                // Moderate Cardinality combination
                colA.Append(rnd.Next(0, 1000));
                colB.Append(rnd.Next(0, 1000));
            }

            _df = new DataFrame(new IColumn[] { colA, colB });
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _df?.Dispose();
        }

        [Benchmark(Description = "MultiCol: GroupBy + Count (Stream)")]
        public int MultiCol_Stream()
        {
            // Erwartung: Allocation < 5 KB (nur Iteratoren und interne Strategie-Wrapper)
            // Statt MB-Bereich für int[] arrays.
            var stream = _df.Lazy()
                            .GroupBy("A", "B")
                            .Agg(F.Count().As("Cnt"))
                            .CollectStream();

            int sum = 0;
            foreach (var row in stream)
            {
                sum += row.Get<int>("Cnt");
            }
            return sum;
        }
    }
}