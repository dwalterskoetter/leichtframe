using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace LeichtFrame.Core.Engine
{
    internal static unsafe class VectorizedHasher
    {
        // Murmur3 32-bit finalizer constants
        private const int C1 = unchecked((int)0x85ebca6b);
        private const int C2 = unchecked((int)0xc2b2ae35);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void HashIntegers(ReadOnlySpan<int> input, Span<int> hashes)
        {
            fixed (int* pInput = input)
            fixed (int* pHashes = hashes)
            {
                int len = input.Length;
                int i = 0;

                if (Avx2.IsSupported && len >= 8)
                {
                    Vector256<int> vC1 = Vector256.Create(C1);
                    Vector256<int> vC2 = Vector256.Create(C2);

                    int loopLimit = len - 8;

                    for (; i <= loopLimit; i += 8)
                    {
                        Vector256<int> k = Avx2.LoadVector256(pInput + i);

                        k = Avx2.Xor(k, Avx2.ShiftRightLogical(k, 16));

                        k = Avx2.MultiplyLow(k, vC1);

                        k = Avx2.Xor(k, Avx2.ShiftRightLogical(k, 13));

                        k = Avx2.MultiplyLow(k, vC2);

                        k = Avx2.Xor(k, Avx2.ShiftRightLogical(k, 16));

                        Avx2.Store(pHashes + i, k);
                    }
                }

                for (; i < len; i++)
                {
                    pHashes[i] = MixScalar(pInput[i]);
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int MixScalar(int k)
        {
            k ^= (int)((uint)k >> 16);
            k *= C1;
            k ^= (int)((uint)k >> 13);
            k *= C2;
            k ^= (int)((uint)k >> 16);
            return k;
        }
    }
}