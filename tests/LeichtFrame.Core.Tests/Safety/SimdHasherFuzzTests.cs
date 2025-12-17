using LeichtFrame.Core.Engine;

namespace LeichtFrame.Core.Tests.Safety
{
    public class SimdHasherFuzzTests
    {
        // Oracle: Exact copy of the scalar fallback logic in VectorizedHasher.
        // Used to verify that SIMD output matches scalar output bit-for-bit.
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
        public void Fuzz_VectorizedHasher_NoCrashes_NoErrors()
        {
            var rnd = new Random(42);
            int iterations = 1000;

            for (int i = 0; i < iterations; i++)
            {
                int length = rnd.Next(0, 2000);

                int[] data = new int[length];
                int[] hashes = new int[length];

                for (int k = 0; k < length; k++)
                {
                    data[k] = rnd.Next();
                }

                // Act: Run the highly optimized SIMD hasher
                VectorizedHasher.HashIntegers(data, hashes);

                // Assert: Verify against the Scalar Oracle
                for (int k = 0; k < length; k++)
                {
                    int expected = ExpectedMix(data[k]);

                    if (hashes[k] != expected)
                    {
                        throw new Exception($"Mismatch at index {k} in iteration {i} (Length: {length}). " +
                                            $"Expected {expected}, got {hashes[k]}. Possible Tail-Processing Bug.");
                    }
                }
            }
        }
    }
}