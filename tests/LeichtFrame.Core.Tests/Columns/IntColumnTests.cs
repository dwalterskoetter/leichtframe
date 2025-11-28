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
            col.Append(0);

            col.SetValue(0, 42);

            Assert.Equal(42, col.Get(0));
        }

        [Fact]
        public void Append_Resizes_Automatically()
        {
            using var col = new IntColumn("Test", capacity: 2);

            col.Append(1);
            col.Append(2);
            col.Append(3);

            Assert.Equal(3, col.Length);
            Assert.Equal(1, col.Get(0));
            Assert.Equal(3, col.Get(2));
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

            col.Append(10);
            col.Append((int?)null);

            Assert.False(col.IsNull(0));
            Assert.True(col.IsNull(1));

            Assert.Equal(0, col.Get(1));
        }

        [Fact]
        public void SetValue_Clears_Null_Flag()
        {
            using var col = new IntColumn("Nullable", 10, isNullable: true);
            col.Append((int?)null);
            Assert.True(col.IsNull(0));

            col.SetValue(0, 99);

            Assert.False(col.IsNull(0));
            Assert.Equal(99, col.Get(0));
        }

        [Fact]
        public void Dispose_Can_Be_Called_Safely()
        {
            var col = new IntColumn("Temp", 10);
            col.Append(1);

            col.Dispose();

            Assert.ThrowsAny<Exception>(() => col.Get(0));
        }
    }
}