namespace LeichtFrame.Core.Tests.DataFrameTests
{
    public class FilterTests
    {
        [Fact]
        public void Where_Filters_Rows_Correctly()
        {
            // Arrange
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("City", typeof(string))
            });
            var df = DataFrame.Create(schema, 10);

            var id = (IntColumn)df["Id"];
            var city = (StringColumn)df["City"];

            // Add Data: 1=Berlin, 2=Munich, 3=Berlin, 4=Hamburg
            id.Append(1); city.Append("Berlin");
            id.Append(2); city.Append("Munich");
            id.Append(3); city.Append("Berlin");
            id.Append(4); city.Append("Hamburg");

            // Act: Filter City == "Berlin"
            var berlinDf = df.Where(row => row.Get<string>("City") == "Berlin");

            // Assert
            Assert.Equal(2, berlinDf.RowCount);
            Assert.Equal(1, berlinDf["Id"].Get<int>(0));
            Assert.Equal(3, berlinDf["Id"].Get<int>(1));
        }

        [Fact]
        public void Where_Handles_Nulls_In_Predicate()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] {
                new ColumnDefinition("Val", typeof(int), IsNullable: true)
            }), 5);
            var col = (IntColumn)df["Val"];

            col.Append(10);
            col.Append(null);
            col.Append(20);

            // Filter: Not null and > 15
            // We need to check if RowView is null-safe or if the user must check.
            // RowView.Get<int> throws on null if T is struct. 
            // Therefore better: use row.GetValue or row.IsNull?
            // User pattern: check null before access.

            var result = df.Where(row =>
            {
                // We use the untyped GetValue here for the null check or catch exception
                // Cleaner: Get the column and check IsNull? No, RowView abstracts that.
                // Solution: User uses Get<int?> (nullable int) if we support that, 
                // OR checks object value.

                object? val = row.GetValue(0);
                return val != null && (int)val > 15;
            });

            Assert.Equal(1, result.RowCount);
            Assert.Equal(20, result["Val"].Get<int>(0));
        }

        [Fact]
        public void Where_Creates_Deep_Copy()
        {
            // Proof that it is not a view (like Slice), but a real copy
            var df = DataFrame.Create(new DataFrameSchema(new[] { new ColumnDefinition("A", typeof(int)) }), 5);
            ((IntColumn)df["A"]).Append(100);

            var filtered = df.Where(r => true); // Copy all

            // Modify the copy
            ((IntColumn)filtered["A"]).SetValue(0, 999);

            // Original must remain unchanged
            Assert.Equal(100, df["A"].Get<int>(0));
        }
    }
}