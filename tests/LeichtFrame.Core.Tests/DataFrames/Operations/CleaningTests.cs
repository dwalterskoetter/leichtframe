namespace LeichtFrame.Core.Tests.DataFrames.Operations
{
    public class CleaningTests
    {
        [Fact]
        public void DropNulls_Removes_Rows_With_Nulls()
        {
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("A", typeof(int), IsNullable: true),
                new ColumnDefinition("B", typeof(string), IsNullable: true)
            });
            var df = DataFrame.Create(schema, 4);
            var a = (IntColumn)df["A"];
            var b = (StringColumn)df["B"];

            // 0: OK
            a.Append(1); b.Append("Hi");
            // 1: Null in A
            a.Append(null); b.Append("Ho");
            // 2: Null in B
            a.Append(2); b.Append(null);
            // 3: OK
            a.Append(3); b.Append("Yi");

            var clean = df.DropNulls();

            Assert.Equal(2, clean.RowCount);
            Assert.Equal(1, clean["A"].Get<int>(0));
            Assert.Equal(3, clean["A"].Get<int>(1));
        }

        [Fact]
        public void DropNulls_Returns_Original_If_No_Nulls()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] { new ColumnDefinition("A", typeof(int)) }), 2);
            ((IntColumn)df["A"]).Append(1);
            ((IntColumn)df["A"]).Append(2);

            var result = df.DropNulls();

            // Should be reference equal optimization
            Assert.Same(df, result);
        }

        [Fact]
        public void FillNull_Replaces_Values_And_Removes_Nullable_Flag()
        {
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Val", typeof(double), IsNullable: true)
            });
            var df = DataFrame.Create(schema, 3);
            var col = (DoubleColumn)df["Val"];

            col.Append(1.5);
            col.Append(null);
            col.Append(3.5);

            // Act: Fill with 0.0
            var filled = df.FillNull("Val", 0.0);

            // Assert
            Assert.Equal(3, filled.RowCount);
            var newCol = (DoubleColumn)filled["Val"];

            Assert.False(newCol.IsNullable); // Should now be non-nullable
            Assert.Equal(1.5, newCol.Get(0));
            Assert.Equal(0.0, newCol.Get(1));
            Assert.Equal(3.5, newCol.Get(2));
        }

        [Fact]
        public void FillNull_Works_With_Strings()
        {
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Txt", typeof(string), IsNullable: true)
            });
            var df = DataFrame.Create(schema, 2);
            ((StringColumn)df["Txt"]).Append(null);
            ((StringColumn)df["Txt"]).Append("B");

            var filled = df.FillNull("Txt", "Empty");

            Assert.Equal("Empty", filled["Txt"].Get<string>(0));
            Assert.Equal("B", filled["Txt"].Get<string>(1));
        }
    }
}