namespace LeichtFrame.Core.Tests.Engine.Algorithms.Hashing
{
    public class VectorizedStringHasherTests
    {
        [Fact]
        public unsafe void HashStrings_Produces_Stable_Hashes()
        {
            byte[] rawBytes = System.Text.Encoding.UTF8.GetBytes("ABCDEF");
            int[] offsets = { 0, 3, 6 };
            int count = 2;
            int[] hashes = new int[count];

            fixed (byte* pBytes = rawBytes)
            fixed (int* pOffsets = offsets)
            fixed (int* pHashes = hashes)
            {
                VectorizedHasher.HashStrings(pBytes, pOffsets, pHashes, count);
            }

            // Assert
            Assert.NotEqual(0, hashes[0]);
            Assert.NotEqual(0, hashes[1]);
            Assert.NotEqual(hashes[0], hashes[1]);
        }

        [Fact]
        public unsafe void HashStrings_Identical_Strings_Produce_Identical_Hashes()
        {
            byte[] rawBytes = System.Text.Encoding.UTF8.GetBytes("TestTest");
            int[] offsets = { 0, 4, 8 };
            int count = 2;
            int[] hashes = new int[count];

            fixed (byte* pBytes = rawBytes)
            fixed (int* pOffsets = offsets)
            fixed (int* pHashes = hashes)
            {
                VectorizedHasher.HashStrings(pBytes, pOffsets, pHashes, count);
            }

            Assert.Equal(hashes[0], hashes[1]);
        }
    }
}