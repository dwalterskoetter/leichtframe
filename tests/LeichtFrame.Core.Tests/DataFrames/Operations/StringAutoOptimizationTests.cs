namespace LeichtFrame.Core.Tests.DataFrames.Operations
{
    [Collection("Sequential")]
    public class StringAutoOptimizationTests
    {
        [Fact]
        public void GroupBy_AutoDetects_LowCardinality_And_Returns_Correct_Strings()
        {
            int n = 1000;
            var col = new StringColumn("Cat", n);
            for (int i = 0; i < n; i++)
            {
                col.Append(i % 2 == 0 ? "A" : "B");
            }
            var df = new DataFrame(new[] { col });

            using var gdf = df.GroupBy("Cat");

            Assert.IsType<DictionaryGroupedDataFrame>(gdf);

            var keys = (string[])gdf.GetKeys();
            Assert.Equal(2, keys.Length);
            Assert.Contains("A", keys);
            Assert.Contains("B", keys);

            var counts = gdf.Count();

            var countA = counts.Where(r => r.Get<string>("Cat") == "A").Columns[1].Get<int>(0);
            var countB = counts.Where(r => r.Get<string>("Cat") == "B").Columns[1].Get<int>(0);

            Assert.Equal(500, countA);
            Assert.Equal(500, countB);
        }

        [Fact]
        public void GroupBy_LowCardinality_With_Many_Rows_Parallel()
        {
            int n = 200_000;
            var col = new StringColumn("Cat", n);
            for (int i = 0; i < n; i++)
            {
                col.Append(i % 4 == 0 ? "A" : "B");
            }
            var df = new DataFrame(new[] { col });

            using var gdf = df.GroupBy("Cat");

            Assert.IsType<DictionaryGroupedDataFrame>(gdf);
            var res = gdf.Count();

            int countA = 0;
            int countB = 0;

            var keyCol = (StringColumn)res["Cat"];
            var valCol = (IntColumn)res["Count"];

            for (int i = 0; i < res.RowCount; i++)
            {
                if (keyCol.Get(i) == "A") countA = valCol.Get(i);
                if (keyCol.Get(i) == "B") countB = valCol.Get(i);
            }

            Assert.Equal(50_000, countA);
            Assert.Equal(150_000, countB);
        }

        [Fact]
        public void GroupBy_Handles_Nulls_Correctly_In_Optimized_Path()
        {
            var col = new StringColumn("Cat", 4, isNullable: true);
            col.Append("A");
            col.Append(null);
            col.Append("A");
            col.Append(null);

            var df = new DataFrame(new[] { col });

            using var gdf = df.GroupBy("Cat");

            Assert.NotNull(gdf.NullGroupIndices);
            Assert.Equal(2, gdf.NullGroupIndices.Length);

            var result = gdf.Count();
            Assert.Equal(2, result.RowCount);

            bool nullFound = false;
            for (int i = 0; i < result.RowCount; i++)
            {
                if (result["Cat"].IsNull(i))
                {
                    Assert.Equal(2, result["Count"].Get<int>(i));
                    nullFound = true;
                }
            }
            Assert.True(nullFound);
        }
    }
}