using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;

namespace LeichtFrame.Benchmarks
{
    [MemoryDiagnoser]
    public class AggregationBenchmarks
    {
        [Params(1_000_000)]
        public int N;

        // LeichtFrame Objects
        private DataFrame _df = null!;

        // LINQ Objects (Poco class to simulate row)
        private List<SalesRecord> _list = null!;

        private record SalesRecord(int BranchId, int Amount);

        [GlobalSetup]
        public void Setup()
        {
            // 1. Setup LeichtFrame
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("BranchId", typeof(int)),
                new ColumnDefinition("Amount", typeof(int))
            });
            _df = DataFrame.Create(schema, N);
            var colBranch = (IntColumn)_df["BranchId"];
            var colAmount = (IntColumn)_df["Amount"];

            // 2. Setup LINQ
            _list = new List<SalesRecord>(N);

            var rnd = new Random(42);
            for (int i = 0; i < N; i++)
            {
                int branch = rnd.Next(0, 100); // 100 Groups
                int amount = rnd.Next(1, 1000);

                colBranch.Append(branch);
                colAmount.Append(amount);

                _list.Add(new SalesRecord(branch, amount));
            }
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _df.Dispose();
        }

        // --- Scenario 1: Global Sum (Span vs LINQ) ---

        [Benchmark(Baseline = true)]
        public long Linq_Sum()
        {
            return _list.Sum(x => (long)x.Amount);
        }

        [Benchmark]
        public double LeichtFrame_Sum()
        {
            // Should be super fast because of Span optimization
            return _df.Sum("Amount");
        }

        // --- Scenario 2: GroupBy + Sum (Dictionary vs LINQ) ---

        [Benchmark]
        public Dictionary<int, long> Linq_GroupBy_Sum()
        {
            return _list.GroupBy(x => x.BranchId)
                        .ToDictionary(g => g.Key, g => g.Sum(x => (long)x.Amount));
        }

        [Benchmark]
        public DataFrame LeichtFrame_GroupBy_Sum()
        {
            // Creates GroupedDataFrame -> Aggregates -> New DataFrame
            return _df.GroupBy("BranchId").Sum("Amount");
        }
    }
}