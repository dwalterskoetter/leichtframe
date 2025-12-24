namespace LeichtFrame.Core.Tests.Safety
{
    public class StringConverterFuzzTests
    {
        [Theory]
        [InlineData(10_000, 10)]
        [InlineData(20_000, 5)]
        [InlineData(20_000, 2000)]
        public void Fuzz_Conversion_Consistency(int rowCount, int cardinality)
        {
            var rnd = new Random(12345);
            string?[] inputs = new string?[rowCount];

            var pool = new string[cardinality];
            for (int i = 0; i < cardinality; i++)
            {
                pool[i] = "Val_" + i + "_" + Guid.NewGuid();
            }

            using var strCol = new StringColumn("Data", rowCount, isNullable: true);

            for (int i = 0; i < rowCount; i++)
            {
                if (rnd.NextDouble() < 0.1)
                {
                    inputs[i] = null;
                    strCol.Append(null);
                }
                else
                {
                    string val = pool[rnd.Next(cardinality)];
                    inputs[i] = val;
                    strCol.Append(val);
                }
            }

            using var catCol = ParallelStringConverter.Convert(strCol);

            Assert.Equal(rowCount, catCol.Length);

            for (int i = 0; i < rowCount; i++)
            {
                string? expected = inputs[i];
                string? actual = catCol.Get(i);

                if (expected == null)
                {
                    Assert.True(catCol.IsNull(i), $"Row {i} should be null");
                    Assert.Null(actual);
                }
                else
                {
                    Assert.False(catCol.IsNull(i), $"Row {i} should not be null");
                    Assert.Equal(expected, actual);
                }
            }

            Assert.True(catCol.Cardinality <= cardinality + 1,
                $"Expected max cardinality {cardinality + 1}, got {catCol.Cardinality}");
        }
    }
}