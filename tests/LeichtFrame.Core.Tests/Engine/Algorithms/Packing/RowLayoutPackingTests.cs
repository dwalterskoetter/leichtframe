using System.Runtime.InteropServices;
using LeichtFrame.Core.Engine.Algorithms.Packing;

namespace LeichtFrame.Core.Tests.Engine.Algorithms.Packing
{
    public class RowLayoutPackingTests
    {
        [Fact]
        public unsafe void Pack_IntegersAndDoubles_CorrectLayout()
        {
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("A", typeof(int)),
                new ColumnDefinition("B", typeof(double))
            });
            var df = DataFrame.Create(schema, 2);

            ((IntColumn)df["A"]).Append(10);
            ((DoubleColumn)df["B"]).Append(20.5);

            ((IntColumn)df["A"]).Append(30);
            ((DoubleColumn)df["B"]).Append(40.5);

            var (bufferPtr, width) = RowLayoutPacking.Pack(df, new[] { "A", "B" });

            try
            {
                Assert.Equal(14, width);

                byte* ptr = (byte*)bufferPtr;

                Assert.Equal(0, *ptr);
                Assert.Equal(10, *(int*)(ptr + 1));

                ptr += 5;
                Assert.Equal(0, *ptr);
                Assert.Equal(20.5, *(double*)(ptr + 1));

                ptr = (byte*)bufferPtr + width;

                Assert.Equal(0, *ptr);
                Assert.Equal(30, *(int*)(ptr + 1));

                ptr += 5;
                Assert.Equal(0, *ptr);
                Assert.Equal(40.5, *(double*)(ptr + 1));
            }
            finally
            {
                NativeMemory.Free((void*)bufferPtr);
            }
        }

        [Fact]
        public unsafe void Pack_Handles_Nulls_Correctly()
        {
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Val", typeof(int), IsNullable: true)
            });
            var df = DataFrame.Create(schema, 2);
            var col = (IntColumn)df["Val"];

            col.Append(100);
            col.Append(null);

            var (bufferPtr, width) = RowLayoutPacking.Pack(df, new[] { "Val" });

            try
            {
                Assert.Equal(5, width);

                byte* ptr = (byte*)bufferPtr;

                Assert.Equal(0, *ptr);
                Assert.Equal(100, *(int*)(ptr + 1));

                ptr += width;
                Assert.Equal(1, *ptr);
                Assert.Equal(0, *(int*)(ptr + 1));
            }
            finally
            {
                NativeMemory.Free((void*)bufferPtr);
            }
        }

        [Fact]
        public unsafe void Pack_Bools_And_DateTimes()
        {
            var now = new DateTime(2023, 1, 1);
            var df = DataFrame.FromObjects(new[]
            {
                new { Active = true, Date = now },
                new { Active = false, Date = now.AddDays(1) }
            });

            var (bufferPtr, width) = RowLayoutPacking.Pack(df, new[] { "Active", "Date" });

            try
            {
                Assert.Equal(11, width);

                byte* ptr = (byte*)bufferPtr;

                Assert.Equal(0, *ptr);
                Assert.Equal(1, *(byte*)(ptr + 1));

                ptr += 2;
                Assert.Equal(0, *ptr);
                Assert.Equal(now.Ticks, *(long*)(ptr + 1));
            }
            finally
            {
                NativeMemory.Free((void*)bufferPtr);
            }
        }
    }
}