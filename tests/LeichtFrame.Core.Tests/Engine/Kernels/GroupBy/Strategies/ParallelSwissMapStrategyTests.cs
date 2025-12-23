namespace LeichtFrame.Core.Tests.Engine.Kernels.GroupBy
{
    public class ParallelSwissMapIntegrationTests
    {
        [Fact]
        public void Parallel_StringGroupBy_LargeDataset_CorrectCounts()
        {
            int rowCount = 600_000;
            var keys = new[] { "A", "B", "C", "D" };

            using var col = new StringColumn("Key", rowCount, isNullable: false);

            for (int i = 0; i < rowCount; i++)
            {
                col.Append(keys[i % 4]);
            }

            var df = new DataFrame(new[] { col });

            var result = df.GroupBy("Key").Count();

            Assert.Equal(4, result.RowCount);

            int expectedCount = rowCount / 4;

            var rowA = result.Where(r => r.Get<string>("Key") == "A");

            Assert.Equal(1, rowA.RowCount);

            Assert.Equal(expectedCount, rowA["Count"].Get<int>(0));

            var rowB = result.Where(r => r.Get<string>("Key") == "B");
            Assert.Equal(1, rowB.RowCount);
            Assert.Equal(expectedCount, rowB["Count"].Get<int>(0));
        }

        [Fact]
        public void Parallel_Path_Ignores_If_Nullable()
        {
            int rowCount = 600_000;
            using var col = new StringColumn("Key", rowCount, isNullable: true);

            for (int i = 0; i < rowCount; i++)
            {
                if (i % 2 == 0) col.Append("A");
                else col.Append(null);
            }

            var df = new DataFrame(new[] { col });

            var result = df.GroupBy("Key").Count();

            Assert.Equal(2, result.RowCount);

            var rowA = result.Where(r => !r.IsNull("Key") && r.Get<string>("Key") == "A");
            Assert.Equal(300_000, rowA["Count"].Get<int>(0));

            bool foundNull = false;
            var countCol = result["Count"];
            var keyCol = result["Key"];
            for (int i = 0; i < result.RowCount; i++)
            {
                if (keyCol.IsNull(i))
                {
                    Assert.Equal(300_000, countCol.GetValue(i));
                    foundNull = true;
                }
            }
            Assert.True(foundNull);
        }
    }
}