using LeichtFrame.Core.Engine.Kernels.GroupBy.Strategies;

namespace LeichtFrame.Core.Tests.Engine.Kernels.GroupBy
{
    public class StringSwissMapStrategyTests
    {
        [Fact]
        public void Group_Strings_HighCardinality_CorrectResults()
        {
            var df = DataFrame.FromObjects(new[]
            {
                new { Dept = "Sales", Val = 1 },
                new { Dept = "IT",    Val = 1 },
                new { Dept = "Sales", Val = 2 },
                new { Dept = "HR",    Val = 1 },
                new { Dept = "IT",    Val = 2 }
            });

            var strategy = new StringSwissMapStrategy();
            using var gdf = strategy.Group(df, "Dept");

            var result = gdf.Count();

            Assert.Equal(3, result.RowCount);
            Assert.True(result.HasColumn("Dept"));
            Assert.True(result.HasColumn("Count"));

            Assert.Equal(typeof(string), result["Dept"].DataType);

            var salesRow = result.Where(r => r.Get<string>("Dept") == "Sales");
            Assert.Equal(1, salesRow.RowCount);
            Assert.Equal(2, salesRow["Count"].Get<int>(0));

            var itRow = result.Where(r => r.Get<string>("Dept") == "IT");
            Assert.Equal(2, itRow["Count"].Get<int>(0));
        }

        [Fact]
        public void Group_Strings_With_Nulls_Works()
        {
            var schema = new DataFrameSchema(new[] { new ColumnDefinition("Txt", typeof(string), IsNullable: true) });
            var df = DataFrame.Create(schema, 4);
            var col = (StringColumn)df["Txt"];

            col.Append("A");
            col.Append(null);
            col.Append("A");
            col.Append(null);

            var strategy = new StringSwissMapStrategy();
            using var gdf = strategy.Group(df, "Txt");

            Assert.Equal(1, gdf.GroupCount);
            Assert.NotNull(gdf.NullGroupIndices);
            Assert.Equal(2, gdf.NullGroupIndices.Length);

            var result = gdf.Count();
            Assert.Equal(2, result.RowCount);

            bool foundNull = false;
            for (int i = 0; i < result.RowCount; i++)
            {
                if (result["Txt"].IsNull(i))
                {
                    foundNull = true;
                    Assert.Equal(2, result["Count"].Get<int>(i));
                }
                else
                {
                    Assert.Equal("A", result["Txt"].Get<string>(i));
                    Assert.Equal(2, result["Count"].Get<int>(i));
                }
            }
            Assert.True(foundNull);
        }
    }
}