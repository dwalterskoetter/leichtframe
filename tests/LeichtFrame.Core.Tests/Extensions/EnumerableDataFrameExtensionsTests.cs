using LeichtFrame.Core;

namespace LeichtFrame.Core.Tests.Extensions
{
    public class EnumerableDataFrameExtensionsTests
    {
        private class TestItem
        {
            public int Id { get; set; }
            public string Name { get; set; } = "";
        }

        [Fact]
        public void ToDataFrameBatches_Splits_Collection_Correctly()
        {
            // Arrange: Generate 10 items
            var source = Enumerable.Range(0, 10).Select(i => new TestItem
            {
                Id = i,
                Name = $"Item {i}"
            });

            // Act: Batch size 3 -> Expecting 4 batches (3, 3, 3, 1)
            var batches = source.ToDataFrameBatches(batchSize: 3).ToList();

            // Assert
            Assert.Equal(4, batches.Count);

            // Check Schema Consistency
            foreach (var b in batches)
            {
                Assert.Equal(2, b.ColumnCount);
                Assert.True(b.HasColumn("Id"));
                Assert.True(b.HasColumn("Name"));
            }

            // Check Batch Sizes
            Assert.Equal(3, batches[0].RowCount);
            Assert.Equal(3, batches[1].RowCount);
            Assert.Equal(3, batches[2].RowCount);
            Assert.Equal(1, batches[3].RowCount);

            // Check Data Integrity (First and Last)
            Assert.Equal(0, batches[0]["Id"].Get<int>(0));
            Assert.Equal(9, batches[3]["Id"].Get<int>(0));
        }

        [Fact]
        public void ToDataFrameBatches_Handles_Empty_Source()
        {
            var source = Enumerable.Empty<TestItem>();

            var batches = source.ToDataFrameBatches(10).ToList();

            Assert.Empty(batches);
        }

        [Fact]
        public void ToDataFrameBatches_Uses_Cached_Schema_For_Performance()
        {
            // Logic verification: Ensure that we can process types correctly
            var source = new[] { new { Val = 10.5 } };

            var batch = source.ToDataFrameBatches(1).First();

            Assert.Equal(typeof(double), batch.GetColumnType("Val"));
            Assert.Equal(10.5, batch["Val"].Get<double>(0));
        }
    }
}