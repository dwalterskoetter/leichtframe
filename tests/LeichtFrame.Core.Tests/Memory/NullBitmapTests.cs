namespace LeichtFrame.Core.Tests.Memory
{
    public class NullBitmapTests
    {
        [Fact]
        public void SetNull_SetsBit_Correctly()
        {
            using var bitmap = new NullBitmap(100);

            bitmap.SetNull(10);

            Assert.True(bitmap.IsNull(10));
            Assert.False(bitmap.IsNull(9));
            Assert.False(bitmap.IsNull(11));
        }

        [Fact]
        public void SetNotNull_ClearsBit_Correctly()
        {
            using var bitmap = new NullBitmap(100);
            bitmap.SetNull(50);
            Assert.True(bitmap.IsNull(50));

            bitmap.SetNotNull(50);
            Assert.False(bitmap.IsNull(50));
        }

        [Fact]
        public void BitLogic_Works_Across_WordBoundaries()
        {
            // A ulong has 64 bits. We test the transition from ulong[0] to ulong[1].
            using var bitmap = new NullBitmap(128);

            bitmap.SetNull(63); // Last bit in the first word
            bitmap.SetNull(64); // First bit in the second word

            Assert.True(bitmap.IsNull(63), "Index 63 failure");
            Assert.True(bitmap.IsNull(64), "Index 64 failure");
            Assert.False(bitmap.IsNull(62));
            Assert.False(bitmap.IsNull(65));
        }

        [Fact]
        public void Resize_Preserves_Existing_Bits()
        {
            using var bitmap = new NullBitmap(64);
            bitmap.SetNull(10);
            bitmap.SetNull(63);

            // Resize to something that requires a new ulong array
            bitmap.Resize(128);

            Assert.True(bitmap.IsNull(10));
            Assert.True(bitmap.IsNull(63));
            Assert.False(bitmap.IsNull(64)); // New area should be empty
        }

        [Fact]
        public void Supports_Large_Indexes_Without_Error()
        {
            // Criteria: "Bitmap behaves correctly for large indexes (e.g., 1 million entries)"
            int largeIndex = 1_000_000;

            // Start small
            using var bitmap = new NullBitmap(16);

            // Resize to large
            bitmap.Resize(largeIndex + 1);

            // Test bit at the end of the large range
            bitmap.SetNull(largeIndex);

            Assert.True(bitmap.IsNull(largeIndex));
            Assert.False(bitmap.IsNull(largeIndex - 1));
        }
    }
}