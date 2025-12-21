namespace LeichtFrame.Core.Tests.DataFrames.Operations
{
    public class MultiColumnSortingTests
    {
        [Fact]
        public void OrderBy_MultiColumn_Integers_Ascending()
        {
            // Scenario:
            // GroupId | Value
            // 1       | 20
            // 1       | 10
            // 2       | 50

            var df = DataFrame.FromObjects(new[]
            {
                new { GroupId = 1, Value = 20 },
                new { GroupId = 1, Value = 10 },
                new { GroupId = 2, Value = 50 }
            });

            // Act: Sort by GroupId (Primary), then Value (Secondary)
            var sorted = df.OrderBy(new[] { "GroupId", "Value" }, new[] { true, true });

            // Assert
            Assert.Equal(3, sorted.RowCount);

            // Row 0: 1, 10
            Assert.Equal(1, sorted["GroupId"].Get<int>(0));
            Assert.Equal(10, sorted["Value"].Get<int>(0));

            // Row 1: 1, 20
            Assert.Equal(1, sorted["GroupId"].Get<int>(1));
            Assert.Equal(20, sorted["Value"].Get<int>(1));

            // Row 2: 2, 50
            Assert.Equal(2, sorted["GroupId"].Get<int>(2));
            Assert.Equal(50, sorted["Value"].Get<int>(2));
        }

        [Fact]
        public void OrderBy_Mixed_Types_String_And_Int()
        {
            // Scenario: Category (String) + Score (Int)
            // "B", 100
            // "A", 50
            // "A", 10

            var df = DataFrame.FromObjects(new[]
            {
                new { Cat = "B", Score = 100 },
                new { Cat = "A", Score = 50 },
                new { Cat = "A", Score = 10 }
            });

            // Act
            var sorted = df.OrderBy(new[] { "Cat", "Score" }, new[] { true, true });

            // Assert
            // "A", 10
            Assert.Equal("A", sorted["Cat"].Get<string>(0));
            Assert.Equal(10, sorted["Score"].Get<int>(0));

            // "A", 50
            Assert.Equal("A", sorted["Cat"].Get<string>(1));
            Assert.Equal(50, sorted["Score"].Get<int>(1));

            // "B", 100
            Assert.Equal("B", sorted["Cat"].Get<string>(2));
        }

        [Fact]
        public void OrderBy_Mixed_Directions_Asc_Desc()
        {
            // Scenario: Department (Asc), Salary (Desc)
            var df = DataFrame.FromObjects(new[]
            {
                new { Dept = "IT", Salary = 1000 },
                new { Dept = "IT", Salary = 5000 },
                new { Dept = "HR", Salary = 2000 }
            });

            // Act: Dept ASC (true), Salary DESC (false)
            var sorted = df.OrderBy(new[] { "Dept", "Salary" }, new[] { true, false });

            // Assert
            // Row 0: HR, 2000 (HR kommt vor IT)
            Assert.Equal("HR", sorted["Dept"].Get<string>(0));

            // Row 1: IT, 5000 (Höheres Gehalt zuerst)
            Assert.Equal("IT", sorted["Dept"].Get<string>(1));
            Assert.Equal(5000, sorted["Salary"].Get<int>(1));

            // Row 2: IT, 1000
            Assert.Equal("IT", sorted["Dept"].Get<string>(2));
            Assert.Equal(1000, sorted["Salary"].Get<int>(2));
        }

        [Fact]
        public void OrderBy_Handles_Nulls_In_Secondary_Column()
        {
            // Scenario:
            // 1, 10
            // 1, null
            // 1, 5

            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Val", typeof(int), IsNullable: true)
            });
            var df = DataFrame.Create(schema, 3);
            var id = (IntColumn)df["Id"];
            var val = (IntColumn)df["Val"];

            id.Append(1); val.Append(10);
            id.Append(1); val.Append(null);
            id.Append(1); val.Append(5);

            // Act
            var sorted = df.OrderBy(new[] { "Id", "Val" }, new[] { true, true });

            // Assert (Nulls first by default in our comparer logic)
            // 1. Null
            Assert.True(sorted["Val"].IsNull(0));

            // 2. 5
            Assert.Equal(5, sorted["Val"].Get<int>(1));

            // 3. 10
            Assert.Equal(10, sorted["Val"].Get<int>(2));
        }
    }
}