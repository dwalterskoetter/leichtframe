using LeichtFrame.Core;

namespace LeichtFrame.Core.Tests.DataFrames.Operations
{
    public class OrderTests
    {
        [Fact]
        public void OrderBy_Integers_Sorts_Correctly()
        {
            // Schema: ID (Int), Name (String)
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Name", typeof(string))
            });
            var df = DataFrame.Create(schema, 5);

            var id = (IntColumn)df["Id"];
            var name = (StringColumn)df["Name"];

            // Unsorted Data
            id.Append(3); name.Append("C");
            id.Append(1); name.Append("A");
            id.Append(2); name.Append("B");

            // Act
            var sorted = df.OrderBy("Id");

            // Assert
            Assert.Equal(3, sorted.RowCount);

            // Check order of ID
            Assert.Equal(1, sorted["Id"].Get<int>(0));
            Assert.Equal(2, sorted["Id"].Get<int>(1));
            Assert.Equal(3, sorted["Id"].Get<int>(2));

            // Check if Name moved with ID (Integrity check)
            Assert.Equal("A", sorted["Name"].Get<string>(0));
            Assert.Equal("B", sorted["Name"].Get<string>(1));
            Assert.Equal("C", sorted["Name"].Get<string>(2));
        }

        [Fact]
        public void OrderByDescending_Strings_Sorts_Correctly()
        {
            var df = DataFrame.FromObjects(new[]
            {
                new { Name = "Alice", Score = 10 },
                new { Name = "Charlie", Score = 30 },
                new { Name = "Bob", Score = 20 }
            });

            // Act
            var sorted = df.OrderByDescending("Name");

            // Assert: Charlie -> Bob -> Alice
            Assert.Equal("Charlie", sorted["Name"].Get<string>(0));
            Assert.Equal("Bob", sorted["Name"].Get<string>(1));
            Assert.Equal("Alice", sorted["Name"].Get<string>(2));

            // Check Score integrity
            Assert.Equal(30, sorted["Score"].Get<int>(0));
        }

        [Fact]
        public void OrderBy_Handles_Nulls_First()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] {
                new ColumnDefinition("Val", typeof(int), IsNullable: true)
            }), 3);

            var col = (IntColumn)df["Val"];
            col.Append(10);
            col.Append(null);
            col.Append(5);

            var sorted = df.OrderBy("Val");

            // Null -> 5 -> 10
            Assert.True(sorted["Val"].IsNull(0));
            Assert.Equal(5, sorted["Val"].Get<int>(1));
            Assert.Equal(10, sorted["Val"].Get<int>(2));
        }

        [Fact]
        public void OrderBy_Chaining_With_Head_Works()
        {
            // Scenario: "Get Top 2 Lowest Prices"
            var df = DataFrame.FromObjects(new[]
            {
                new { Price = 100 },
                new { Price = 50 },
                new { Price = 10 },
                new { Price = 500 }
            });

            var result = df.OrderBy("Price").Head(2);

            Assert.Equal(2, result.RowCount);
            Assert.Equal(10, result["Price"].Get<int>(0));
            Assert.Equal(50, result["Price"].Get<int>(1));
        }
    }
}