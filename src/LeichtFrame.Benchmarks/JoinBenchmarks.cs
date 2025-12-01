using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;

namespace LeichtFrame.Benchmarks
{
    [MemoryDiagnoser]
    public class JoinBenchmarks
    {
        [Params(100_000)] // We start with 100k, as joins are expensive (for 1M possibly adjust params)
        public int N;

        // LeichtFrame
        private DataFrame _dfLeft = null!;
        private DataFrame _dfRight = null!;

        // LINQ
        private List<RecordLeft> _listLeft = null!;
        private List<RecordRight> _listRight = null!;

        // POCOs für LINQ
        record RecordLeft(int Id, int ValueLeft);
        record RecordRight(int Id, int ValueRight);

        [GlobalSetup]
        public void Setup()
        {
            // 1. Setup LeichtFrame
            var schemaLeft = new DataFrameSchema(new[] { new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("ValL", typeof(int)) });
            var schemaRight = new DataFrameSchema(new[] { new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("ValR", typeof(int)) });

            _dfLeft = DataFrame.Create(schemaLeft, N);
            _dfRight = DataFrame.Create(schemaRight, N);

            var lId = (IntColumn)_dfLeft["Id"]; var lVal = (IntColumn)_dfLeft["ValL"];
            var rId = (IntColumn)_dfRight["Id"]; var rVal = (IntColumn)_dfRight["ValR"];

            // 2. Setup LINQ
            _listLeft = new List<RecordLeft>(N);
            _listRight = new List<RecordRight>(N);

            for (int i = 0; i < N; i++)
            {
                // 1:1 Match for maximum stress
                lId.Append(i); lVal.Append(i * 10);
                rId.Append(i); rVal.Append(i * 20);

                _listLeft.Add(new RecordLeft(i, i * 10));
                _listRight.Add(new RecordRight(i, i * 20));
            }
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _dfLeft.Dispose();
            _dfRight.Dispose();
        }

        [Benchmark(Baseline = true)]
        public int Linq_Join()
        {
            // Standard LINQ Hash Join
            var result = _listLeft.Join(_listRight,
                left => left.Id,
                right => right.Id,
                (left, right) => new { left.Id, left.ValueLeft, right.ValueRight })
                .ToList(); // Materialize!

            return result.Count;
        }

        [Benchmark]
        public DataFrame LeichtFrame_Join()
        {
            // Unser Hash Join
            return _dfLeft.Join(_dfRight, on: "Id", JoinType.Inner);
        }
    }
}