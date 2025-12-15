namespace LeichtFrame.Core.Tests.Engine.Hashing
{
    public class VectorizedHasherTests
    {
        private static int ExpectedMix(int k)
        {
            const int C1 = unchecked((int)0x85ebca6b);
            const int C2 = unchecked((int)0xc2b2ae35);

            k ^= (int)((uint)k >> 16);
            k *= C1;
            k ^= (int)((uint)k >> 13);
            k *= C2;
            k ^= (int)((uint)k >> 16);
            return k;
        }

        [Fact]
        public void HashIntegers_Produces_Deterministic_Results()
        {
            int[] data = { 1, 2, 3, 100, -50 };
            int[] hashes1 = new int[data.Length];
            int[] hashes2 = new int[data.Length];

            VectorizedHasher.HashIntegers(data, hashes1);
            VectorizedHasher.HashIntegers(data, hashes2);

            Assert.Equal(hashes1, hashes2);
        }

        [Fact]
        public void HashIntegers_Consistency_Between_SIMD_And_Scalar()
        {
            int count = 1024;
            int[] data = new int[count];
            for (int i = 0; i < count; i++) data[i] = i * 100;

            int[] results = new int[count];

            VectorizedHasher.HashIntegers(data, results);

            for (int i = 0; i < count; i++)
            {
                int expected = ExpectedMix(data[i]);
                Assert.Equal(expected, results[i]);
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(7)]
        [InlineData(8)]
        [InlineData(9)]
        [InlineData(15)]
        [InlineData(16)]
        [InlineData(31)]
        public void HashIntegers_Handles_Ragged_Lengths(int length)
        {
            int[] data = Enumerable.Range(0, length).Select(x => x * 3).ToArray();
            int[] hashes = new int[length];

            VectorizedHasher.HashIntegers(data, hashes);

            for (int i = 0; i < length; i++)
            {
                int expected = ExpectedMix(data[i]);
                Assert.Equal(expected, hashes[i]);
            }
        }
    }
}