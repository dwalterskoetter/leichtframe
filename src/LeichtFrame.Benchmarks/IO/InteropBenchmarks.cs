using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using LeichtFrame.Core;
using LeichtFrame.IO;

namespace LeichtFrame.Benchmarks
{
    [MemoryDiagnoser]
    [Orderer(SummaryOrderPolicy.FastestToSlowest)]
    public class InteropBenchmarks : BenchmarkData
    {
        private RecordBatch _arrowBatch = null!;

        public override void GlobalSetup()
        {
            base.GlobalSetup();
            // Pre-calculate an Arrow Batch to measure Import speed
            _arrowBatch = _lfFrame.ToArrow();
        }

        // =========================================================
        // EXPORT TO ARROW
        // =========================================================

        [Benchmark(Description = "LeichtFrame ToArrow (Export)")]
        public RecordBatch LF_ToArrow()
        {
            // Measures how fast we can map internal columns to Arrow arrays
            return _lfFrame.ToArrow();
        }

        // =========================================================
        // IMPORT FROM ARROW
        // =========================================================

        [Benchmark(Description = "LeichtFrame FromArrow (Import)")]
        public DataFrame LF_FromArrow()
        {
            // Measures how fast we can ingest Arrow data into LeichtFrame columns
            return _arrowBatch.ToDataFrame();
        }
    }
}