namespace LeichtFrame.Core.Tests.DataFrame
{
    public class RowViewTests
    {
        [Fact]
        public void RowView_Access_Works_Typed_And_Untyped()
        {
            // Setup
            using var intCol = new IntColumn("Age", 10);
            intCol.Append(42);

            using var strCol = new StringColumn("Name", 10);
            strCol.Append("Alice");

            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Age", typeof(int)),
                new ColumnDefinition("Name", typeof(string))
            });

            var columns = new IColumn[] { intCol, strCol };
            var row = new RowView(0, columns, schema);

            // 1. Typed Access (via IColumn<T>.GetValue)
            Assert.Equal(42, row.Get<int>(0));
            Assert.Equal("Alice", row.Get<string>("Name"));

            // 2. Untyped Access (via IColumn.GetValue)
            Assert.Equal(42, row.GetValue(0));     // GetValue methode
            Assert.Equal("Alice", row["Name"]);    // Indexer
        }
    }
}