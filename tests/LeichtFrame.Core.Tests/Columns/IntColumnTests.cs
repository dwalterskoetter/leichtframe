using System;
using Xunit;
using LeichtFrame.Core;

namespace LeichtFrame.Core.Tests.Columns
{
    public class IntColumnTests
    {
        [Fact]
        public void Constructor_Sets_IsNullable_Correctly()
        {
            using var col1 = new IntColumn("A", 10, isNullable: false);
            using var col2 = new IntColumn("B", 10, isNullable: true);

            Assert.False(col1.IsNullable);
            Assert.True(col2.IsNullable);
        }

        [Fact]
        public void SetValue_And_GetValue_Work()
        {
            using var col = new IntColumn("Test", 10);
            col.Append(0); // Dummy init for Length 1

            col.SetValue(0, 42);
            Assert.Equal(42, col.GetValue(0));
        }

        [Fact]
        public void Append_Resizes_Automatically()
        {
            // Start with capacity 2
            using var col = new IntColumn("Test", capacity: 2);

            col.Append(1);
            col.Append(2);
            col.Append(3); // Here it must resize

            Assert.Equal(3, col.Length);
            Assert.Equal(1, col.GetValue(0));
            Assert.Equal(3, col.GetValue(2));
        }

        [Fact]
        public void NonNullable_Column_Throws_On_SetNull()
        {
            using var col = new IntColumn("Strict", 10, isNullable: false);
            col.Append(1);

            Assert.Throws<InvalidOperationException>(() => col.SetNull(0));
        }

        [Fact]
        public void Nullable_Column_Can_Store_Nulls()
        {
            using var col = new IntColumn("Nullable", 10, isNullable: true);

            col.Append(10);       // Index 0
            col.Append((int?)null); // Index 1

            Assert.False(col.IsNull(0));
            Assert.True(col.IsNull(1));

            // Default value for null is usually 0 (default(int)) in the buffer
            Assert.Equal(0, col.GetValue(1));
        }

        [Fact]
        public void SetValue_Clears_Null_Flag()
        {
            using var col = new IntColumn("Nullable", 10, isNullable: true);
            col.Append((int?)null); // Create null
            Assert.True(col.IsNull(0));

            col.SetValue(0, 99); // Overwrite with value

            Assert.False(col.IsNull(0));
            Assert.Equal(99, col.GetValue(0));
        }

        [Fact]
        public void Dispose_Can_Be_Called_Safely()
        {
            var col = new IntColumn("Temp", 10);
            col.Append(1);

            col.Dispose();

            // Check if accessing after dispose throws (which it should, since _data is null)
            // Note: Behavior after accessing disposed object is not strictly defined,
            // but NullReferenceException is expected since we set _data = null.
            Assert.ThrowsAny<Exception>(() => col.GetValue(0));
        }
    }
}