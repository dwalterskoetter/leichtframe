using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using LeichtFrame.Core;
using MDA = Microsoft.Data.Analysis;

namespace LeichtFrame.Benchmarks
{
    public record DataItem(int Id, int Val);

    [SimpleJob(RuntimeMoniker.Net80)]
    [MemoryDiagnoser]
    public class BattleRoyaleBenchmarks
    {
        [Params(1_000_000)]
        public int N;

        // --- LINQ ---
        private List<int> _linqRawInts = null!;
        private List<DataItem> _linqObjs = null!;
        private List<DataItem> _linqObjsRight = null!;

        // --- LeichtFrame ---
        private LeichtFrame.Core.DataFrame _lfFrame = null!;
        private LeichtFrame.Core.DataFrame _lfFrameRight = null!;

        // --- Microsoft ---
        private MDA.DataFrame _mdaFrame = null!;
        private MDA.DataFrame _mdaFrameRight = null!;

        [GlobalSetup]
        public void Setup()
        {
            var rnd = new Random(42);
            var dataIds = new int[N];
            var dataVals = new int[N];

            for (int i = 0; i < N; i++)
            {
                dataIds[i] = i;
                dataVals[i] = rnd.Next(100);
            }

            // --- 1. LINQ Setup ---
            _linqRawInts = new List<int>(dataVals);
            _linqObjs = dataIds.Zip(dataVals, (id, val) => new DataItem(id, val)).ToList();
            _linqObjsRight = new List<DataItem>(_linqObjs);

            // --- 2. LeichtFrame Setup ---
            // Schema Links
            var schemaLeft = new DataFrameSchema(new[] {
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Val", typeof(int))
            });

            // Schema Rechts
            var schemaRight = new DataFrameSchema(new[] {
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Val_Right", typeof(int))
            });

            _lfFrame = LeichtFrame.Core.DataFrame.Create(schemaLeft, N);
            var lfId = (IntColumn)_lfFrame["Id"];
            var lfVal = (IntColumn)_lfFrame["Val"];

            _lfFrameRight = LeichtFrame.Core.DataFrame.Create(schemaRight, N);
            var lfIdR = (IntColumn)_lfFrameRight["Id"];
            var lfValR = (IntColumn)_lfFrameRight["Val_Right"];

            // Append
            for (int i = 0; i < N; i++)
            {
                lfId.Append(dataIds[i]);
                lfVal.Append(dataVals[i]);

                lfIdR.Append(dataIds[i]);
                lfValR.Append(dataVals[i]);
            }

            // --- 3. Microsoft Setup ---
            var mdaId = new MDA.PrimitiveDataFrameColumn<int>("Id", N);
            var mdaVal = new MDA.PrimitiveDataFrameColumn<int>("Val", N);
            for (int i = 0; i < N; i++) { mdaId[i] = dataIds[i]; mdaVal[i] = dataVals[i]; }
            _mdaFrame = new MDA.DataFrame(mdaId, mdaVal);

            var mdaIdR = new MDA.PrimitiveDataFrameColumn<int>("Id", N);
            var mdaValR = new MDA.PrimitiveDataFrameColumn<int>("Val", N);
            for (int i = 0; i < N; i++) { mdaIdR[i] = dataIds[i]; mdaValR[i] = dataVals[i]; }
            _mdaFrameRight = new MDA.DataFrame(mdaIdR, mdaValR);
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _lfFrame.Dispose();
            _lfFrameRight.Dispose();
        }

        // --- AGGREGATION ---
        [Benchmark(Description = "Sum (Agg)")]
        public double Agg_LeichtFrame() => _lfFrame.Sum("Val");

        [Benchmark(Baseline = true, Description = "Sum (LINQ List<int>)")]
        public long Agg_LINQ_Raw() => _linqRawInts.Sum(x => (long)x);

        [Benchmark(Description = "Sum (MS Data Analysis)")]
        public double Agg_Microsoft()
        {
            return Convert.ToDouble(_mdaFrame["Val"].Sum());
        }

        // --- FILTER ---
        [Benchmark(Description = "Filter (Where > 50)")]
        public object Filter_LeichtFrame() => _lfFrame.Where(row => row.Get<int>("Val") > 50);

        [Benchmark(Description = "Filter (LINQ Objects)")]
        public object Filter_LINQ_Obj() => _linqObjs.Where(x => x.Val > 50).ToList();

        [Benchmark(Description = "Filter (MS Data Analysis)")]
        public object Filter_Microsoft()
        {
            var col = _mdaFrame.Columns["Val"];
            var boolFilter = col.ElementwiseGreaterThan(50);
            return _mdaFrame.Filter(boolFilter);
        }

        // --- JOIN ---
        [Benchmark(Description = "Join (Inner on Id)")]
        public object Join_LeichtFrame()
        {
            return _lfFrame.Join(_lfFrameRight, "Id", JoinType.Inner);
        }

        [Benchmark(Description = "Join (LINQ Objects)")]
        public object Join_LINQ_Obj()
        {
            return _linqObjs.Join(
                _linqObjsRight,
                left => left.Id,
                right => right.Id,
                (left, right) => new { Left = left, Right = right }
            ).ToList();
        }

        [Benchmark(Description = "Join (MS Data Analysis)")]
        public object Join_Microsoft()
        {
            return _mdaFrame.Merge(_mdaFrameRight, new[] { "Id" }, new[] { "Id" });
        }
    }
}