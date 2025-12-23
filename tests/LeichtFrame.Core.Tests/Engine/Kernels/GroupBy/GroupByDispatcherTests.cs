namespace LeichtFrame.Core.Tests.Engine
{
    public class GroupByDispatcherTests
    {
        [Fact]
        public void Dispatcher_ShouldHandle_DenseData_Correctly()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] { new ColumnDefinition("A", typeof(int)) }), 100);
            var col = (IntColumn)df["A"];
            for (int i = 0; i < 100; i++) col.Append(i % 10);

            var result = df.GroupBy("A").Count();

            Assert.Equal(10, result.RowCount);
            Assert.Equal(10, result["Count"].Get<int>(0));
        }

        [Fact]
        public void Dispatcher_ShouldHandle_SparseData_Correctly()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] { new ColumnDefinition("A", typeof(int)) }), 2);
            var col = (IntColumn)df["A"];
            col.Append(0);
            col.Append(2_000_000);

            var result = df.GroupBy("A").Count();

            Assert.Equal(2, result.RowCount);

            var bigRow = result.Where(r => r.Get<int>("A") == 2_000_000);
            Assert.Equal(1, bigRow.RowCount);
        }

        [Fact]
        public void GroupBy_On_StringColumn_Uses_Optimized_Path()
        {
            var df = DataFrame.FromObjects(new[] {
                new { Id = "A" },
                new { Id = "B" },
                new { Id = "A" }
            });

            var result = df.GroupBy("Id").Count();

            Assert.Equal(2, result.RowCount);
            Assert.Equal(2, result.Where(r => r.Get<string>("Id") == "A")["Count"].Get<int>(0));
        }

        [Fact]
        public void GroupBy_MultiColumn_Primitives_Uses_RowLayout_Path()
        {
            var df = DataFrame.FromObjects(new[]
            {
                new { A = 1, B = true }
            });

            var gdf = df.GroupBy("A", "B");
            var result = gdf.Count();

            Assert.Equal(1, result.RowCount);
            Assert.Equal(1, result["A"].Get<int>(0));
            Assert.True(result["B"].Get<bool>(0));
        }
    }
}