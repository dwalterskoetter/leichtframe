using LeichtFrame.Core.Engine.Kernels.GroupBy.Strategies;

namespace LeichtFrame.Core.Tests.Engine.Kernels.GroupBy
{
    public class RowLayoutHashStrategyTests
    {
        [Fact]
        public void Group_MultiColumn_Int_Double_Works()
        {
            var df = DataFrame.FromObjects(new[]
            {
                new { A = 1, B = 1.1 },
                new { A = 1, B = 1.1 },
                new { A = 1, B = 2.2 },
                new { A = 2, B = 1.1 }
            });

            var strategy = new RowLayoutHashStrategy();

            using var gdf = strategy.Group(df, new[] { "A", "B" });

            Assert.Equal(3, gdf.GroupCount);

            var counts = gdf.Count();

            Assert.Equal(3, counts.RowCount);

            var g1 = counts.Where(r => r.Get<int>("A") == 1 && r.Get<double>("B") == 1.1);
            Assert.Equal(1, g1.RowCount);
            Assert.Equal(2, g1["Count"].Get<int>(0));

            var g2 = counts.Where(r => r.Get<int>("A") == 1 && r.Get<double>("B") == 2.2);
            Assert.Equal(1, g2.RowCount);
            Assert.Equal(1, g2["Count"].Get<int>(0));
        }

        [Fact]
        public void Group_MultiColumn_With_Nulls_Distinguishes_Correctly()
        {
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("A", typeof(int)),
                new ColumnDefinition("B", typeof(int), IsNullable: true)
            });
            var df = DataFrame.Create(schema, 4);
            var a = (IntColumn)df["A"];
            var b = (IntColumn)df["B"];

            a.Append(1); b.Append(0);
            a.Append(1); b.Append(null);
            a.Append(1); b.Append(null);
            a.Append(1); b.Append(0);

            var strategy = new RowLayoutHashStrategy();
            using var gdf = strategy.Group(df, new[] { "A", "B" });

            var counts = gdf.Count();

            Assert.Equal(2, counts.RowCount);

            var nullGroup = counts.Where(r => r.IsNull("B"));
            Assert.Equal(2, nullGroup["Count"].Get<int>(0));

            var zeroGroup = counts.Where(r => !r.IsNull("B") && r.Get<int>("B") == 0);
            Assert.Equal(2, zeroGroup["Count"].Get<int>(0));
        }
    }
}