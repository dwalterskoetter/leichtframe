using LeichtFrame.Core;

namespace LeichtFrame.Core.Tests.DataFrames.Operations
{
    public class SortingTests
    {
        [Fact]
        public void GetSortedIndices_Sorts_Integers_Ascending()
        {
            using var col = new IntColumn("Data", 5);
            col.Append(10); // 0
            col.Append(50); // 1
            col.Append(5);  // 2
            col.Append(20); // 3

            // Expected Order Values: 5, 10, 20, 50
            // Expected Order Indices: 2, 0, 3, 1

            var indices = col.GetSortedIndices(ascending: true);

            Assert.Equal(4, indices.Length);
            Assert.Equal(2, indices[0]);
            Assert.Equal(0, indices[1]);
            Assert.Equal(3, indices[2]);
            Assert.Equal(1, indices[3]);
        }

        [Fact]
        public void GetSortedIndices_Sorts_Integers_Descending()
        {
            using var col = new IntColumn("Data", 5);
            col.Append(10); // 0
            col.Append(50); // 1
            col.Append(5);  // 2

            // Expected Order Values: 50, 10, 5
            // Expected Order Indices: 1, 0, 2

            var indices = col.GetSortedIndices(ascending: false);

            Assert.Equal(1, indices[0]);
            Assert.Equal(0, indices[1]);
            Assert.Equal(2, indices[2]);
        }

        [Fact]
        public void GetSortedIndices_Handles_Nulls_First()
        {
            using var col = new StringColumn("Text", 5, isNullable: true);
            col.Append("B");    // 0
            col.Append(null);   // 1
            col.Append("A");    // 2

            // Standard: Nulls are smallest -> Null, A, B
            // Indices: 1, 2, 0

            var indices = col.GetSortedIndices(ascending: true);

            Assert.Equal(1, indices[0]);
            Assert.Equal(2, indices[1]);
            Assert.Equal(0, indices[2]);
        }

        [Fact]
        public void GetSortedIndices_Doubles_Correctness()
        {
            using var col = new DoubleColumn("Vals", 5);
            col.Append(1.1); // 0
            col.Append(0.9); // 1
            col.Append(1.1); // 2 (Duplicate)

            var indices = col.GetSortedIndices(ascending: true);

            // 0.9 comes first
            Assert.Equal(1, indices[0]);

            // For duplicates, order is not guaranteed (unstable sort), 
            // but indices must be valid (either 0 then 2, or 2 then 0).
            bool validOrder = (indices[1] == 0 && indices[2] == 2) || (indices[1] == 2 && indices[2] == 0);
            Assert.True(validOrder);
        }
    }
}