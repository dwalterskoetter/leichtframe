using LeichtFrame.Core;

namespace LeichtFrame.Core.Tests.Engine.GroupBy
{
    public class RadixSortTests
    {
        [Fact]
        public void Radix_Grouping_Matches_Standard_Grouping()
        {
            // Arrange
            int count = 1000;
            using var col = new IntColumn("Id", count);
            var rnd = new Random(42);

            // Generate data with clusters (0, 0, 1, 2, 2, 2...)
            for (int i = 0; i < count; i++)
                col.Append(rnd.Next(0, 50)); // 50 unique groups approx.

            var df = new DataFrame(new[] { col });

            // Act 1: Standard (Reference)
            var stdGroup = df.GroupByStandard("Id");
            var stdCounts = stdGroup.Count();

            // Act 2: Radix (God Mode)
            var radixGroup = df.GroupByRadix("Id");
            var radixCounts = radixGroup.Count();

            // Assert
            Assert.Equal(stdGroup.GroupCount, radixGroup.GroupCount);
            Assert.Equal(stdCounts.RowCount, radixCounts.RowCount);

            // Check if Sum of Counts matches total rows (Basic integrity)
            long totalStd = (long)stdCounts.Sum("Count");
            long totalRadix = (long)radixCounts.Sum("Count");

            Assert.Equal(count, totalStd);
            Assert.Equal(count, totalRadix);
        }
    }
}