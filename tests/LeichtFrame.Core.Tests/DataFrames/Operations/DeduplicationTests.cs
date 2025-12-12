using LeichtFrame.Core;

namespace LeichtFrame.Core.Tests.DataFrames.Operations
{
    public class DeduplicationTests
    {
        [Fact]
        public void Distinct_Removes_Exact_Duplicates()
        {
            var df = DataFrame.FromObjects(new[]
            {
                new { Id = 1, Name = "A" },
                new { Id = 2, Name = "B" }, // Duplicate follows
                new { Id = 2, Name = "B" },
                new { Id = 3, Name = "C" }
            });

            var unique = df.Distinct();

            Assert.Equal(3, unique.RowCount);
            Assert.Equal(1, unique["Id"].Get<int>(0));
            Assert.Equal(2, unique["Id"].Get<int>(1));
            Assert.Equal(3, unique["Id"].Get<int>(2));
        }

        [Fact]
        public void Distinct_By_Subset_Keeps_First_Occurrence()
        {
            var df = DataFrame.FromObjects(new[]
            {
                new { Group = "X", Val = 10 },
                new { Group = "X", Val = 99 }, // Same group, different val
                new { Group = "Y", Val = 20 }
            });

            // Distinct by Group only -> Should keep row 1 and 3
            var result = df.Distinct("Group");

            Assert.Equal(2, result.RowCount);
            Assert.Equal("X", result["Group"].Get<string>(0));
            Assert.Equal(10, result["Val"].Get<int>(0)); // First one kept
            Assert.Equal("Y", result["Group"].Get<string>(1));
        }

        [Fact]
        public void Distinct_Handles_Nulls()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] {
                new ColumnDefinition("A", typeof(string), IsNullable: true)
            }), 4);
            var col = (StringColumn)df["A"];

            col.Append("Hi");
            col.Append(null);
            col.Append("Hi");
            col.Append(null);

            var unique = df.Distinct();

            Assert.Equal(2, unique.RowCount);
            // Assuming "Hi" comes first, then null (order of insertion preserved)
            Assert.Equal("Hi", unique["A"].Get<string>(0));
            Assert.True(unique["A"].IsNull(1));
        }

        [Fact]
        public void Distinct_No_Duplicates_Returns_Original_Reference()
        {
            // Optimization Check: If no duplicates found, return same object (if implemented)
            var df = DataFrame.FromObjects(new[] { new { A = 1 }, new { A = 2 } });

            var result = df.Distinct();

            Assert.Same(df, result);
        }
    }
}