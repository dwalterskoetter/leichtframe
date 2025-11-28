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
        public void Interning_Deduplicates_References()
        {
            // AC 3 Check
            using var col = new StringColumn("Interned", 10, useInterning: true);

            string s1 = new string(new char[] { 'A', 'B' }); // "AB" (new reference)
            string s2 = new string(new char[] { 'A', 'B' }); // "AB" (different reference)

            // Prove they are different objects initially
            Assert.False(object.ReferenceEquals(s1, s2));

            col.Append(s1);
            col.Append(s2);

            string? out1 = col.Get(0);
            string? out2 = col.Get(1);

            // After interning, they should point to the same object
            Assert.True(object.ReferenceEquals(out1, out2));
            Assert.Equal("AB", out1);
        }

        [Fact]
        public void Memory_Estimate_Returns_Plausible_Values()
        {
            // AC 4 Check
            using var col = new StringColumn("Mem", 100);
            col.Append("Hello"); // 5 chars * 2 = 10 bytes + overhead

            long est = col.EstimateMemoryUsage();

            // Minimal check: Array overhead + string content > 0
            Assert.True(est > 0);
            Assert.True(est > (10 + IntPtr.Size));
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