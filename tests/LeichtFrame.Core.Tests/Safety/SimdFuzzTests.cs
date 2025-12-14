using LeichtFrame.Core;

namespace LeichtFrame.Core.Tests.Safety
{
    public class SimdFuzzTests
    {
        private const int ITERATIONS = 100;

        public static IEnumerable<object[]> RaggedLengths()
        {
            var lengths = new List<int> {
                0, 1, 3, 4, 5,
                7, 8, 9,
                15, 16, 17,
                31, 32, 33,
                127, 128, 129,
                1000
            };

            foreach (var l in lengths) yield return new object[] { l };
        }

        [Theory]
        [MemberData(nameof(RaggedLengths))]
        public void Fuzz_Int_Aggregations_SumMinMax(int length)
        {
            var rnd = new Random(42);

            for (int i = 0; i < ITERATIONS; i++)
            {
                int[] data = new int[length];
                for (int j = 0; j < length; j++)
                {

                    double r = rnd.NextDouble();
                    if (r < 0.1) data[j] = 0;
                    else if (r < 0.2) data[j] = int.MaxValue;
                    else if (r < 0.3) data[j] = int.MinValue;
                    else if (r < 0.4) data[j] = rnd.Next(-100, 100);
                    else data[j] = rnd.Next();
                }

                using var col = new IntColumn("Fuzz", length);
                foreach (var val in data) col.Append(val);

                if (length > 0)
                {
                    int expectedMin = data.Min();
                    int actualMin = col.Min();
                    Assert.Equal(expectedMin, actualMin);

                    int expectedMax = data.Max();
                    int actualMax = col.Max();
                    Assert.Equal(expectedMax, actualMax);
                }

                long expectedSum = data.Select(x => (long)x).Sum();
                long actualSum = col.Sum();

                Assert.Equal(expectedSum, actualSum);
            }
        }

        [Theory]
        [MemberData(nameof(RaggedLengths))]
        public void Fuzz_Double_Aggregations_With_NaN(int length)
        {
            var rnd = new Random(123);

            for (int i = 0; i < ITERATIONS; i++)
            {
                double[] data = new double[length];
                for (int j = 0; j < length; j++)
                {
                    double r = rnd.NextDouble();
                    if (r < 0.05) data[j] = double.NaN;
                    else if (r < 0.1) data[j] = double.PositiveInfinity;
                    else if (r < 0.15) data[j] = 0.0;
                    else data[j] = (rnd.NextDouble() * 10000) - 5000;
                }

                using var col = new DoubleColumn("FuzzDbl", length);
                foreach (var val in data) col.Append(val);

                if (length > 0)
                {
                    double expectedSum = data.Sum();

                    double actualSum = col.Sum();

                    if (double.IsNaN(expectedSum))
                    {
                        Assert.True(double.IsNaN(actualSum), "Sum should be NaN if input contains NaN");
                    }
                    else
                    {
                        Assert.Equal(expectedSum, actualSum, precision: 2);
                    }
                }
            }
        }

        [Fact]
        public void Fuzz_WhereVec_Int_Filter()
        {
            var rnd = new Random(999);
            int N = 1000;

            for (int k = 0; k < 50; k++)
            {
                int[] data = new int[N];
                for (int i = 0; i < N; i++) data[i] = rnd.Next(-100, 100);

                var schema = new DataFrameSchema(new[] { new ColumnDefinition("Val", typeof(int)) });
                var df = DataFrame.Create(schema, N);
                var col = (IntColumn)df["Val"];
                foreach (var v in data) col.Append(v);

                int threshold = rnd.Next(-50, 50);

                var expectedIndices = data
                    .Select((val, idx) => new { val, idx })
                    .Where(x => x.val > threshold)
                    .Select(x => x.idx)
                    .ToList();

                var resultDf = df.WhereVec("Val", CompareOp.GreaterThan, threshold);

                Assert.Equal(expectedIndices.Count, resultDf.RowCount);

                var resCol = (IntColumn)resultDf["Val"];
                for (int r = 0; r < resultDf.RowCount; r++)
                {
                    int originalVal = data[expectedIndices[r]];
                    Assert.Equal(originalVal, resCol.Get(r));
                }
            }
        }

        [Fact]
        public void Fuzz_Arithmetic_Int_Add()
        {
            var rnd = new Random(555);
            int len = 130;

            for (int k = 0; k < 20; k++)
            {
                int[] arrA = new int[len];
                int[] arrB = new int[len];

                for (int i = 0; i < len; i++)
                {
                    arrA[i] = rnd.Next(-1000, 1000);
                    arrB[i] = rnd.Next(-1000, 1000);
                }

                using var cA = new IntColumn("A", len);
                using var cB = new IntColumn("B", len);
                foreach (var v in arrA) cA.Append(v);
                foreach (var v in arrB) cB.Append(v);

                using var res = cA + cB;

                for (int i = 0; i < len; i++)
                {
                    Assert.Equal(arrA[i] + arrB[i], res.Get(i));
                }
            }
        }

        [Fact]
        public void Boundary_Check_Allocated_Memory()
        {
            using var c1 = new IntColumn("C1", 7);
            for (int i = 0; i < 7; i++) c1.Append(i);

            var sum = c1.Sum();
            Assert.Equal(21, sum);
        }
    }
}