namespace LeichtFrame.Core.Tests.Columns
{
    public class CategoryColumnTests
    {
        [Fact]
        public void Append_And_Get_Work_Correctly()
        {
            using var col = new CategoryColumn("Cat", 10);

            col.Append("A");
            col.Append("B");
            col.Append("A");
            col.Append("C");

            Assert.Equal(4, col.Length);
            Assert.Equal("A", col.Get(0));
            Assert.Equal("B", col.Get(1));
            Assert.Equal("A", col.Get(2));
            Assert.Equal("C", col.Get(3));

            Assert.Equal(4, col.Cardinality);
        }

        [Fact]
        public void Handles_Null_Values()
        {
            using var col = new CategoryColumn("Cat");

            col.Append("A");
            col.Append(null);
            col.Append("A");

            Assert.Equal(3, col.Length);
            Assert.Equal("A", col.Get(0));
            Assert.Null(col.Get(1));

            Assert.True(col.IsNull(1));
            Assert.False(col.IsNull(0));
        }

        [Fact]
        public void CloneSubset_Shares_Dictionary_Instance_Flyweight()
        {
            using var col = new CategoryColumn("Cat");
            col.Append("A");
            col.Append("B");
            col.Append("C");
            col.Append("A");

            var subsetIndices = new[] { 1, 2 };
            using var clone = (CategoryColumn)col.CloneSubset(subsetIndices);

            Assert.Equal(2, clone.Length);
            Assert.Equal("B", clone.Get(0));
            Assert.Equal("C", clone.Get(1));

            Assert.Same(col.InternalDictionary, clone.InternalDictionary);

            Assert.Equal(col.Cardinality, clone.Cardinality);
        }

        [Fact]
        public void CreateFromInternals_Reconstructs_Lookup()
        {
            int[] codes = new int[] { 1, 2, 1, 0 };
            var dict = new List<string?> { null, "A", "B" };

            using var col = CategoryColumn.CreateFromInternals("Reconstructed", codes, dict);

            Assert.Equal(4, col.Length);
            Assert.Equal("A", col.Get(0));
            Assert.Equal("B", col.Get(1));
            Assert.Equal("A", col.Get(2));
            Assert.Null(col.Get(3));
            Assert.True(col.IsNull(3));

            col.Append("A");
            Assert.Equal(1, col.Codes.Get(4));

            col.Append("C");
            Assert.Equal(3, col.Codes.Get(5));
            Assert.Equal("C", col.Get(5));
        }
    }
}