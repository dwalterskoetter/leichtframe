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

        [Fact]
        public void Group_Count_Returns_Correct_DataFrame()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] { new ColumnDefinition("Dept", typeof(string)) }), 10);
            var col = (StringColumn)df["Dept"];
            col.Append("IT");
            col.Append("Sales");
            col.Append("IT");

            var result = df.GroupBy("Dept").Count();

            Assert.Equal(2, result.RowCount);

            // Verify Structure
            Assert.Equal("Dept", result.Columns[0].Name);
            Assert.Equal("Count", result.Columns[1].Name);

            // Verify Data (Order is not guaranteed with HashMap, so we find rows)
            // Simpler check for MVP:
            // "IT" -> 2, "Sales" -> 1

            // Quick workaround to verify content without Order-dependency logic:
            var itRow = result.Where(r => r.Get<string>("Dept") == "IT");
            Assert.Equal(2, itRow["Count"].Get<int>(0));
        }

        [Fact]
        public void Group_Sum_Calculates_Totals()
        {
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Val", typeof(double))
            });
            var df = DataFrame.Create(schema, 10);

            var id = (IntColumn)df["Id"];
            var val = (DoubleColumn)df["Val"];

            // Group 1: 10 + 20 = 30
            id.Append(1); val.Append(10.0);
            id.Append(1); val.Append(20.0);

            // Group 2: 5 = 5
            id.Append(2); val.Append(5.0);

            var result = df.GroupBy("Id").Sum("Val");

            Assert.Equal(2, result.RowCount);

            // Check Sum for ID 1
            var g1 = result.Where(r => r.Get<int>("Id") == 1);
            Assert.Equal(30.0, g1["Sum_Val"].Get<double>(0));
        }

        [Fact]
        public void Group_Sum_Handles_Null_Values_In_Data()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] {
                new ColumnDefinition("G", typeof(string)),
                new ColumnDefinition("V", typeof(int), IsNullable: true)
            }), 5);

            var g = (StringColumn)df["G"];
            var v = (IntColumn)df["V"];

            g.Append("A"); v.Append(10);
            g.Append("A"); v.Append(null); // Should be ignored
            g.Append("A"); v.Append(5);

            var result = df.GroupBy("G").Sum("V");

            Assert.Equal(15.0, result["Sum_V"].Get<double>(0));
        }
    }
}