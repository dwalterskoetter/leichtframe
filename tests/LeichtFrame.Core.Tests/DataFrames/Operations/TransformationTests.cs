namespace LeichtFrame.Core.Tests.DataFrames.Operations
{
    public class TransformationTests
    {
        [Fact]
        public void AddColumn_Calculates_Simple_Math()
        {
            // Schema: Item, Price, Qty
            var df = DataFrame.FromObjects(new[]
            {
                new { Item = "A", Price = 10.0, Qty = 2 },
                new { Item = "B", Price = 5.5, Qty = 4 }
            });

            // Act: Total = Price * Qty
            var result = df.AddColumn("Total", row =>
                row.Get<double>("Price") * row.Get<int>("Qty")
            );

            // Assert
            Assert.Equal(4, result.ColumnCount); // 3 old + 1 new
            Assert.True(result.HasColumn("Total"));

            Assert.Equal(20.0, result["Total"].Get<double>(0));
            Assert.Equal(22.0, result["Total"].Get<double>(1));
        }

        [Fact]
        public void AddColumn_Works_With_Strings()
        {
            var df = DataFrame.FromObjects(new[]
            {
                new { First = "Hans", Last = "Müller" },
                new { First = "Alice", Last = "Wonder" }
            });

            // Act: FullName = First + " " + Last
            var result = df.AddColumn("FullName", row =>
                $"{row.Get<string>("First")} {row.Get<string>("Last")}"
            );

            Assert.Equal("Hans Müller", result["FullName"].Get<string>(0));
            Assert.Equal("Alice Wonder", result["FullName"].Get<string>(1));
        }

        [Fact]
        public void AddColumn_Supports_Nullable_Result()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] { new ColumnDefinition("Val", typeof(int)) }), 2);
            ((IntColumn)df["Val"]).Append(10);
            ((IntColumn)df["Val"]).Append(0); // Div by zero potential?

            // Act: Add nullable double column
            // Logic: If Val > 5 return Val/2, else null
            var result = df.AddColumn<double?>("Half", row =>
            {
                int v = row.Get<int>("Val");
                return v > 5 ? v / 2.0 : null;
            });

            Assert.True(result["Half"].IsNullable);
            Assert.Equal(5.0, result["Half"].Get<double>(0));
            Assert.True(result["Half"].IsNull(1));
        }

        [Fact]
        public void AddColumn_Throws_If_Name_Exists()
        {
            var df = DataFrame.FromObjects(new[] { new { Id = 1 } });

            Assert.Throws<ArgumentException>(() =>
                df.AddColumn("Id", r => r.Get<int>("Id") + 1)
            );
        }
    }
}