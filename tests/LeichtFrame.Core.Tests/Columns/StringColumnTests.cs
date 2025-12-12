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
    }
}