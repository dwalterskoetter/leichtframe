namespace LeichtFrame.Core.Tests.Columns
{
    public class BoolColumnTests
    {
        [Fact]
        public void BitPacking_Works_Across_ByteBoundaries()
        {
            // Capacity 16 = 2 Bytes
            using var col = new BoolColumn("Bits", 16);

            // Set index 0, 7 (Byte 0 ends), 8 (Byte 1 starts)
            for (int i = 0; i < 10; i++) col.Append(false);

            col.SetValue(0, true);
            col.SetValue(7, true);
            col.SetValue(8, true);

            Assert.True(col.Get(0));
            Assert.True(col.Get(7));
            Assert.True(col.Get(8));

            Assert.False(col.Get(1));
            Assert.False(col.Get(6));
            Assert.False(col.Get(9));
        }

        [Fact]
        public void AnyTrue_And_AllTrue_Logic()
        {
            using var col = new BoolColumn("Logic", 100);

            // 1. Empty/All False
            col.Append(false);
            col.Append(false);
            Assert.False(col.AnyTrue());
            Assert.False(col.AllTrue());

            // 2. Set one true
            col.SetValue(0, true);
            Assert.True(col.AnyTrue());
            Assert.False(col.AllTrue());

            // 3. Set all true
            col.SetValue(1, true);
            Assert.True(col.AllTrue());
        }

        [Fact]
        public void Logic_Ignores_Nulls()
        {
            using var col = new BoolColumn("NullLogic", 10, isNullable: true);

            col.Append(true);
            col.Append((bool?)null); // Should be ignored

            Assert.True(col.AllTrue()); // True because the only valid value is true
            Assert.True(col.AnyTrue());

            col.Append(false);
            Assert.False(col.AllTrue()); // Now we have a false
        }

        [Fact]
        public void Values_Property_Throws_Exception()
        {
            using var col = new BoolColumn("NoSlice", 10);
            Assert.Throws<NotSupportedException>(() => _ = col.Values);
        }

        [Fact]
        public void Resizing_Preserves_Bits()
        {
            using var col = new BoolColumn("Resize", 8); // 1 Byte
            for (int i = 0; i < 8; i++) col.Append(true); // Fill byte with 1s (255)

            col.Append(false); // Trigger resize to 2nd byte

            Assert.Equal(9, col.Length);
            Assert.True(col.Get(0));
            Assert.True(col.Get(7));
            Assert.False(col.Get(8));
        }
    }
}