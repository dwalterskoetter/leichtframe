using LeichtFrame.Core;

namespace LeichtFrame.Core.Tests.DataFrames.Operations
{
    public class LeftJoinTests
    {
        [Fact]
        public void LeftJoin_Preserves_Unmatched_Left_Rows()
        {
            // Left: Users (1, 2, 3)
            var left = DataFrame.FromObjects(new[]
            {
                new { Id = 1, Name = "Alice" },
                new { Id = 2, Name = "Bob" },
                new { Id = 3, Name = "Charlie" }
            });

            // Right: Orders (Only User 1 and 3 bought something)
            var right = DataFrame.FromObjects(new[]
            {
                new { Id = 1, Product = "Book" },
                new { Id = 3, Product = "Car" }
            });

            // Act
            var result = left.Join(right, "Id", JoinType.Left);

            // Assert
            Assert.Equal(3, result.RowCount); // All 3 users must be there

            // Check Alice (Match)
            Assert.Equal("Alice", result["Name"].Get<string>(0));
            Assert.Equal("Book", result["Product"].Get<string>(0));

            // Check Bob (No Match) - Product should be null
            Assert.Equal("Bob", result["Name"].Get<string>(1));
            Assert.True(result["Product"].IsNull(1));

            // Check Charlie (Match)
            Assert.Equal("Charlie", result["Name"].Get<string>(2));
            Assert.Equal("Car", result["Product"].Get<string>(2));
        }

        [Fact]
        public void LeftJoin_Converts_Right_IntColumn_To_Nullable()
        {
            // Left
            var left = DataFrame.Create(new DataFrameSchema(new[] { new ColumnDefinition("K", typeof(int)) }), 2);
            ((IntColumn)left["K"]).Append(1);
            ((IntColumn)left["K"]).Append(2);

            // Right: Has INT Value (Non-Nullable initially)
            // But after Left Join, it must support nulls for the missing row.
            var right = DataFrame.Create(new DataFrameSchema(new[]{
                new ColumnDefinition("K", typeof(int)),
                new ColumnDefinition("Val", typeof(int), IsNullable: false)
            }), 1);

            ((IntColumn)right["K"]).Append(1);
            ((IntColumn)right["Val"]).Append(100);

            // Act
            var result = left.Join(right, "K", JoinType.Left);

            // Assert
            Assert.Equal(2, result.RowCount);

            var valCol = result["Val"];
            Assert.True(valCol.IsNullable, "Right column must become nullable");

            // Row 1 (Match): 100
            Assert.Equal(100, valCol.Get<int>(0));

            // Row 2 (No Match): Null
            Assert.True(valCol.IsNull(1));
        }

        [Fact]
        public void LeftJoin_Handles_Duplicates_On_Right()
        {
            // 1:N Relationship
            var left = DataFrame.FromObjects(new[] { new { K = 1 } });
            var right = DataFrame.FromObjects(new[]
            {
                new { K = 1, V = "A" },
                new { K = 1, V = "B" }
            });

            var result = left.Join(right, "K", JoinType.Left);

            Assert.Equal(2, result.RowCount);
            Assert.Equal("A", result["V"].Get<string>(0));
            Assert.Equal("B", result["V"].Get<string>(1));
        }
    }
}