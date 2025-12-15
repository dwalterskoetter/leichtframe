namespace LeichtFrame.Core.Tests.Columns
{
    public class StringColumnTests
    {
        [Fact]
        public void Basic_Append_Read()
        {
            using var col = new StringColumn("Names", 10);
            col.Append("Alice");
            col.Append("Bob");

            Assert.Equal(2, col.Length);
            Assert.Equal("Alice", col.Get(0));
            Assert.Equal("Bob", col.Get(1));
        }

        [Fact]
        public void Nullable_String_Works()
        {
            using var col = new StringColumn("Nullable", 10, isNullable: true);
            col.Append("Text");
            col.Append(null);

            Assert.False(col.IsNull(0));
            Assert.True(col.IsNull(1));
            Assert.Null(col.Get(1));
        }

        [Fact]
        public void Dispose_Clears_References()
        {
            // As we use ArrayPool, it is difficult to directly test 
            // if the array was cleared, as we lose the reference.
            // But we check that Dispose does not throw an exception.
            var col = new StringColumn("Temp", 10);
            col.Append("Foo");
            col.Dispose();
        }

        [Fact]
        public void CompareRaw_Sorts_Bytes_Correctly()
        {
            using var col = new StringColumn("SortTest", 5, isNullable: true);

            col.Append("A");        // 0
            col.Append("B");        // 1
            col.Append("AA");       // 2
            col.Append("a");        // 3 (ASCII 97 > 65)
            col.Append(null);       // 4

            // 1. A < B
            Assert.Equal(-1, Math.Sign(col.CompareRaw(0, 1)));

            // 2. B > A
            Assert.Equal(1, Math.Sign(col.CompareRaw(1, 0)));

            // 3. A < AA (Prefix Logik)
            Assert.Equal(-1, Math.Sign(col.CompareRaw(0, 2)));

            // 4. A < a (Case Sensitivity: 'A'=65, 'a'=97)
            Assert.Equal(-1, Math.Sign(col.CompareRaw(0, 3)));

            // 5. Null Handling
            // Null (4) vs A (0)
            Assert.Equal(-1, col.CompareRaw(4, 0));
            // A (0) vs Null (4)
            Assert.Equal(1, col.CompareRaw(0, 4));
            // Null vs Null -> 0
            Assert.Equal(0, col.CompareRaw(4, 4));
        }
    }
}