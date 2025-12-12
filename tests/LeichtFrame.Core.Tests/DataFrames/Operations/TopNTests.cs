namespace LeichtFrame.Core.Tests.DataFrames.Operations
{
    public class TopNTests
    {
        [Fact]
        public void Smallest_Returns_Correct_Items_Sorted()
        {
            var df = DataFrame.FromObjects(new[]
            {
                new { Val = 50 },
                new { Val = 10 },
                new { Val = 100 },
                new { Val = 5 },
                new { Val = 20 }
            });

            // Smallest 3: 5, 10, 20
            var result = df.Smallest(3, "Val");

            Assert.Equal(3, result.RowCount);
            Assert.Equal(5, result["Val"].Get<int>(0));
            Assert.Equal(10, result["Val"].Get<int>(1));
            Assert.Equal(20, result["Val"].Get<int>(2));
        }

        [Fact]
        public void Largest_Returns_Correct_Items_Sorted_Descending()
        {
            var df = DataFrame.FromObjects(new[]
            {
                new { Val = 1 },
                new { Val = 99 },
                new { Val = 2 },
                new { Val = 100 },
                new { Val = 3 }
            });

            // Largest 2: 100, 99
            var result = df.Largest(2, "Val");

            Assert.Equal(2, result.RowCount);
            Assert.Equal(100, result["Val"].Get<int>(0));
            Assert.Equal(99, result["Val"].Get<int>(1));
        }

        [Fact]
        public void Smallest_LargerThanRowCount_Returns_FullSort()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] { new ColumnDefinition("A", typeof(int)) }), 2);
            ((IntColumn)df["A"]).Append(20);
            ((IntColumn)df["A"]).Append(10);

            var result = df.Smallest(10, "A"); // Request 10, have 2

            Assert.Equal(2, result.RowCount);
            Assert.Equal(10, result["A"].Get<int>(0));
            Assert.Equal(20, result["A"].Get<int>(1));
        }

        [Fact]
        public void Largest_Strings_Works()
        {
            var df = DataFrame.FromObjects(new[]
            {
                new { Name = "A" },
                new { Name = "Z" },
                new { Name = "C" }
            });

            var result = df.Largest(1, "Name");

            Assert.Equal(1, result.RowCount);
            Assert.Equal("Z", result["Name"].Get<string>(0));
        }
    }
}