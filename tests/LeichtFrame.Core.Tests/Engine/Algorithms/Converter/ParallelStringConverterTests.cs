namespace LeichtFrame.Core.Tests.Engine.Algorithms
{
    [Collection("Sequential")]
    public class ParallelStringConverterTests
    {
        [Fact]
        public void Convert_SmallData_UsesSequentialFallback_And_Correct()
        {
            using var col = new StringColumn("Data", 100, isNullable: true);
            col.Append("A");
            col.Append("B");
            col.Append("A");
            col.Append(null);

            using var cat = ParallelStringConverter.Convert(col);

            Assert.Equal(4, cat.Length);
            Assert.Equal("A", cat.Get(0));
            Assert.Equal("B", cat.Get(1));
            Assert.Equal("A", cat.Get(2));
            Assert.Null(cat.Get(3));
            Assert.True(cat.IsNull(3));

            Assert.Equal(3, cat.Cardinality);
        }

        [Fact]
        public void Convert_LargeData_UsesParallelPath_And_Correct()
        {
            int n = 150_000;
            using var col = new StringColumn("Data", n, isNullable: true);

            for (int i = 0; i < n; i++)
            {
                if (i % 3 == 0) col.Append("Apple");
                else if (i % 3 == 1) col.Append("Banana");
                else col.Append(null);
            }

            using var cat = ParallelStringConverter.Convert(col);

            Assert.Equal(n, cat.Length);

            Assert.Equal("Apple", cat.Get(0));
            Assert.Equal("Banana", cat.Get(1));
            Assert.Null(cat.Get(2));

            Assert.Equal("Apple", cat.Get(n - 3));
            Assert.Equal("Banana", cat.Get(n - 2));
            Assert.Null(cat.Get(n - 1));

            Assert.Equal(3, cat.Cardinality);
        }

        [Fact]
        public void Convert_Handles_EmptyStrings_Distinct_From_Null()
        {
            int n = 200_000;
            using var col = new StringColumn("Data", n, isNullable: true);

            col.Append("");
            col.Append(null);
            for (int i = 2; i < n; i++) col.Append("Val");

            using var cat = ParallelStringConverter.Convert(col);

            Assert.Equal("", cat.Get(0));
            Assert.False(cat.IsNull(0));

            Assert.Null(cat.Get(1));
            Assert.True(cat.IsNull(1));
        }

        [Fact]
        public void Convert_HighCardinality_DoesNotCrash()
        {
            int n = 120_000;
            using var col = new StringColumn("Data", n, isNullable: true);
            for (int i = 0; i < n; i++) col.Append(i.ToString());

            using var cat = ParallelStringConverter.Convert(col);

            Assert.Equal(n, cat.Length);
            Assert.Equal("0", cat.Get(0));
            Assert.Equal("119999", cat.Get(n - 1));
        }
    }
}