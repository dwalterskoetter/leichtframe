using static LeichtFrame.Core.Expressions.F;

namespace LeichtFrame.Core.Tests.Lazy
{
    public class LazyExecutionTests
    {
        [Fact]
        public void Collect_Materializes_Simple_Select()
        {
            // Arrange
            var df = DataFrame.FromObjects(new[]
            {
                new { Id = 1, Name = "Alice" },
                new { Id = 2, Name = "Bob" }
            });

            // Act: Lazy Select without transformation
            var result = df.Lazy()
                           .Select(Col("Name"), Col("Id"))
                           .Collect();

            // Assert
            Assert.Equal(2, result.RowCount);
            Assert.Equal("Alice", result["Name"].Get<string>(0));
            Assert.Equal(1, result["Id"].Get<int>(0));
        }

        [Fact]
        public void Arithmetic_Transformation_Int()
        {
            // Arrange
            var df = DataFrame.FromObjects(new[] { new { Val = 10 }, new { Val = 20 } });

            // Act: Val * 2 + 5
            var result = df.Lazy()
                           .Select((Col("Val") * 2 + 5).As("Result"))
                           .Collect();

            // Assert
            var col = result["Result"];
            Assert.Equal(25, col.Get<int>(0)); // 10*2 + 5
            Assert.Equal(45, col.Get<int>(1)); // 20*2 + 5
        }

        [Fact]
        public void Arithmetic_Transformation_Double()
        {
            // Arrange
            var df = DataFrame.FromObjects(new[] { new { Price = 10.0 } });

            // Act: Price * 1.5
            var result = df.Lazy()
                           .Select((Col("Price") * 1.5).As("NewPrice"))
                           .Collect();

            // Assert
            Assert.Equal(15.0, result["NewPrice"].Get<double>(0));
        }

        [Fact]
        public void Where_Filter_Pushdown_Simulation()
        {
            // Arrange
            var df = DataFrame.FromObjects(new[]
            {
                new { Id = 1 },
                new { Id = 10 },
                new { Id = 5 }
            });

            // Act: Filter > 5
            var result = df.Lazy()
                           .Where(Col("Id") > 5)
                           .Select(Col("Id"))
                           .Collect();

            // Assert
            Assert.Equal(1, result.RowCount);
            Assert.Equal(10, result["Id"].Get<int>(0));
        }

        [Fact]
        public void Chained_Operations_Execution()
        {
            // Complex Chain: Filter -> Calc -> Rename
            var df = DataFrame.FromObjects(new[]
            {
                new { A = 10, B = 2 },
                new { A = 5,  B = 2 }, // Filtered out
                new { A = 20, B = 4 }
            });

            var result = df.Lazy()
                           .Where(Col("A") > 5)
                           .Select(
                               (Col("A") / Col("B")).As("DivResult")
                           )
                           .Collect();

            // Assert
            Assert.Equal(2, result.RowCount);

            // Row 1: 10 / 2 = 5
            Assert.Equal(5, result["DivResult"].Get<int>(0));

            // Row 2: 20 / 4 = 5
            Assert.Equal(5, result["DivResult"].Get<int>(1));
        }

        [Fact]
        public void Literal_Column_Creation()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] { new ColumnDefinition("A", typeof(int)) }), 3);
            ((IntColumn)df["A"]).Append(0);
            ((IntColumn)df["A"]).Append(0);
            ((IntColumn)df["A"]).Append(0);

            var result = df.Lazy()
                           .Select(Col("A"), Lit(99).As("Constant"))
                           .Collect();

            Assert.Equal(3, result.RowCount);
            Assert.Equal(99, result["Constant"].Get<int>(0));
            Assert.Equal(99, result["Constant"].Get<int>(2));
        }
    }
}