using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using LeichtFrame.Core;
using LeichtFrame.Core.Expressions;

namespace LeichtFrame.Benchmarks.Breakdown
{
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    public class StringStreamingBenchmarks
    {
        [Params(1_000_000)]
        public int N;

        private DataFrame _df = null!;

        [GlobalSetup]
        public void Setup()
        {
            // High Cardinality: Unique UUIDs
            // Zwingt die Engine, die StringSwissMapStrategy zu nutzen
            // und nicht die Category-Optimierung.
            var colUuid = new StringColumn("UUID", N);

            for (int i = 0; i < N; i++)
            {
                colUuid.Append(Guid.NewGuid().ToString());
            }

            _df = new DataFrame(new IColumn[] { colUuid });
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _df?.Dispose();
        }

        [Benchmark(Description = "String: GroupBy (HighCard) + Count (Stream)")]
        public int String_Stream()
        {
            // Wir erwarten Zero Alloc im Streaming, trotz 1 Mio Strings.
            // Die Strings selbst werden nicht materialisiert, nur Pointer.
            var stream = _df.Lazy()
                            .GroupBy("UUID")
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