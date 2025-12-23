using LeichtFrame.Core.Engine.Kernels.GroupBy.Strategies;

namespace LeichtFrame.Core.Tests.Engine
{
    public class IntSwissMapStrategyTests
    {
        [Fact]
        public void Group_With_IntSwissMap_Produces_Correct_CSR()
        {
            var df = DataFrame.FromObjects(new[]
            {
                new { Val = 10 },
                new { Val = 20 },
                new { Val = 10 },
                new { Val = 30 },
                new { Val = 20 }
            });

            var strategy = new IntSwissMapStrategy();
            using var gdf = strategy.Group(df, "Val");

            Assert.Equal(3, gdf.GroupCount);

            var counts = gdf.Count();

            var row10 = counts.Where(r => r.Get<int>("Val") == 10);
            Assert.Equal(2, row10["Count"].Get<int>(0));

            var row20 = counts.Where(r => r.Get<int>("Val") == 20);
            Assert.Equal(2, row20["Count"].Get<int>(0));

            var row30 = counts.Where(r => r.Get<int>("Val") == 30);
            Assert.Equal(1, row30["Count"].Get<int>(0));
        }

        [Fact]
        public void Group_With_Nulls_Separates_Null_Correctly()
        {
            var schema = new DataFrameSchema(new[] { new ColumnDefinition("ID", typeof(int), IsNullable: true) });
            var df = DataFrame.Create(schema, 4);
            var col = (IntColumn)df["ID"];

            col.Append(1);
            col.Append(null);
            col.Append(1);
            col.Append(null);

            var strategy = new IntSwissMapStrategy();
            using var gdf = strategy.Group(df, "ID");

            Assert.Equal(1, gdf.GroupCount);
            Assert.NotNull(gdf.NullGroupIndices);
            Assert.Equal(2, gdf.NullGroupIndices.Length);

            var result = gdf.Count();
            Assert.Equal(2, result.RowCount);

            bool foundNull = false;
            for (int i = 0; i < result.RowCount; i++)
            {
                if (result["ID"].IsNull(i))
                {
                    foundNull = true;
                    Assert.Equal(2, result["Count"].Get<int>(i));
                }
            }
            Assert.True(foundNull);
        }
    }
}