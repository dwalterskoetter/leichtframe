namespace LeichtFrame.Core.Tests.DataFrames.Operations
{
    public class SlicingTests
    {
        [Fact]
        public void Slice_Creates_Correct_Window_On_Data()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] { new ColumnDefinition("Num", typeof(int)) }), 10);
            var col = (IntColumn)df["Num"];
            for (int i = 0; i < 5; i++) col.Append(i * 10); // 0, 10, 20, 30, 40

            // Slice middle: index 1 to 3 (Length 2) -> [10, 20]
            var slice = df.Slice(1, 2);

            Assert.Equal(2, slice.RowCount);
            Assert.Equal(10, slice["Num"].Get<int>(0)); // Original Index 1
            Assert.Equal(20, slice["Num"].Get<int>(1)); // Original Index 2
        }

        [Fact]
        public void Slice_Is_ZeroCopy_WriteThrough()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] { new ColumnDefinition("Num", typeof(int)) }), 5);
            ((IntColumn)df["Num"]).Append(100);
            ((IntColumn)df["Num"]).Append(200);

            var slice = df.Slice(1, 1); // View on row 1 (Value 200)

            // Change value via Slice
            var sliceCol = (IColumn<int>)slice["Num"];
            sliceCol.SetValue(0, 999);

            // Verify Change in Original
            Assert.Equal(999, df["Num"].Get<int>(1));
        }

        [Fact]
        public void Head_And_Tail_Work_As_Expected()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] { new ColumnDefinition("Id", typeof(int)) }), 10);
            for (int i = 0; i < 10; i++) ((IntColumn)df["Id"]).Append(i);

            var head = df.Head(3);
            Assert.Equal(3, head.RowCount);
            Assert.Equal(0, head["Id"].Get<int>(0));
            Assert.Equal(2, head["Id"].Get<int>(2));

            var tail = df.Tail(2);
            Assert.Equal(2, tail.RowCount);
            Assert.Equal(8, tail["Id"].Get<int>(0));
            Assert.Equal(9, tail["Id"].Get<int>(1));
        }

        [Fact]
        public void Slice_Handles_Out_Of_Bounds_Gracefully()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] { new ColumnDefinition("A", typeof(int)) }), 5);
            ((IntColumn)df["A"]).Append(1); // 1 Row total

            var safeSlice = df.Slice(0, 100); // Request more than exists
            Assert.Equal(1, safeSlice.RowCount); // Should clamp to real count

            var emptySlice = df.Slice(100, 5); // Start way after end
            Assert.Equal(0, emptySlice.RowCount);
        }
    }
}