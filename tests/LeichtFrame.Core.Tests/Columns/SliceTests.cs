using System;
using Xunit;
using LeichtFrame.Core;

namespace LeichtFrame.Core.Tests.Columns
{
    public class SliceTests
    {
        [Fact]
        public void Slice_Returns_Correct_SubSet()
        {
            using var col = new IntColumn("Data", 10);
            col.Append(10);
            col.Append(20);
            col.Append(30);
            col.Append(40);
            col.Append(50);

            // Slice from index 1, length 3 -> [20, 30, 40]
            var slice = col.Slice(1, 3);

            Assert.Equal(3, slice.Length);
            Assert.Equal(20, slice.Span[0]);
            Assert.Equal(40, slice.Span[2]);
        }

        [Fact]
        public void Slice_Is_ZeroCopy()
        {
            // Slice operations do not allocate copies
            using var col = new IntColumn("ZeroCopy", 10);
            col.Append(100);
            col.Append(200);

            var slice = col.Slice(0, 2);

            // Modify ORIGINAL column
            col.SetValue(1, 999);

            // Verify SLICE sees the change (proof that it points to same memory)
            Assert.Equal(999, slice.Span[1]);
        }

        [Fact]
        public void Slice_Throws_On_Invalid_Bounds()
        {
            // Slice throws on invalid bounds
            using var col = new IntColumn("Bounds", 5);
            col.Append(1);
            col.Append(2);

            // Length is 2
            Assert.Throws<ArgumentOutOfRangeException>(() => col.Slice(0, 3)); // Too long
            Assert.Throws<ArgumentOutOfRangeException>(() => col.Slice(2, 1)); // Start at end
            Assert.Throws<ArgumentOutOfRangeException>(() => col.Slice(-1, 1)); // Negative start
        }

        [Fact]
        public void BoolColumn_Throws_NotSupported_On_Slice()
        {
            // BoolColumn special case
            using var col = new BoolColumn("Bools", 8);
            col.Append(true);

            Assert.Throws<NotSupportedException>(() => col.Slice(0, 1));
        }

        [Fact]
        public void StringColumn_Slice_Works()
        {
            using var col = new StringColumn("Strings", 5);
            col.Append("A");
            col.Append("B");
            col.Append("C");

            var slice = col.Slice(1, 1);
            Assert.Equal("B", slice.Span[0]);
        }
    }
}