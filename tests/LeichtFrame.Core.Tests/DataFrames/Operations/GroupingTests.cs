namespace LeichtFrame.Core.Tests.DataFrameTests
{
    public class GroupingTests
    {
        [Fact]
        public void GroupBy_Strings_creates_Correct_Buckets()
        {
            // Arrange
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Dept", typeof(string)),
                new ColumnDefinition("Id", typeof(int))
            });
            var df = DataFrame.Create(schema, 10);

            var dept = (StringColumn)df["Dept"];
            dept.Append("Sales"); // 0
            dept.Append("IT");    // 1
            dept.Append("Sales"); // 2
            dept.Append("HR");    // 3
            dept.Append("IT");    // 4

            // Act
            var grouped = df.GroupBy("Dept");

            // Assert
            Assert.Equal(3, grouped.GroupMap.Count); // Sales, IT, HR

            // Check Sales bucket
            Assert.True(grouped.GroupMap.ContainsKey("Sales"));
            var salesIndices = grouped.GroupMap["Sales"];
            Assert.Equal(new[] { 0, 2 }, salesIndices);

            // Check IT bucket
            var itIndices = grouped.GroupMap["IT"];
            Assert.Equal(new[] { 1, 4 }, itIndices);
        }

        [Fact]
        public void GroupBy_Handles_Nulls_As_Separate_Group()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] {
                new ColumnDefinition("Cat", typeof(string), IsNullable: true)
            }), 5);
            var col = (StringColumn)df["Cat"];

            col.Append("A");
            col.Append(null);
            col.Append("A");
            col.Append(null);

            var grouped = df.GroupBy("Cat");

            Assert.Equal(2, grouped.GroupMap.Count); // "A" and NullKey

            // We need to check indirectly since NullKey is internal/private.
            // We iterate over keys and find the one that is not "A".
            var nullGroupKey = grouped.GroupMap.Keys.First(k => k is not string);
            var indices = grouped.GroupMap[nullGroupKey];

            Assert.Equal(new[] { 1, 3 }, indices);
        }

        [Fact]
        public void GroupBy_Integers_Works()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] { new ColumnDefinition("Num", typeof(int)) }), 5);
            var col = (IntColumn)df["Num"];
            col.Append(10);
            col.Append(20);
            col.Append(10);

            var grouped = df.GroupBy("Num");

            Assert.Equal(2, grouped.GroupMap.Count); // 2 Groups overall (10 and 20)
            Assert.Equal(2, grouped.GroupMap[10].Count); // Group 10 has 2 entries
            Assert.Single(grouped.GroupMap[20]); // Group 20 has exactly 1 entry
        }
    }
}