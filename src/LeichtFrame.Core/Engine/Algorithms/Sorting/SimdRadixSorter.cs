using System.Buffers;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace LeichtFrame.Core.Engine
{
    internal static unsafe class SimdRadixSorter
    {
        private const int RadixBits = 11;
        private const int BucketCount = 1 << RadixBits; // 2048
        private const int Mask = BucketCount - 1;

        /// <summary>
        /// Sorts the indices based on the provided hash values using a SIMD-optimized Radix Sort algorithm.
        /// </summary>
        public static void Sort(int* pHashes, int* pIndices, int* pTempIndices, int length)
        {
            int parallelism = Environment.ProcessorCount;

            // 1. Init
            InitializeIndicesSIMD(pIndices, length);

            // 2. Histograms
            int[][] histograms = new int[parallelism][];
            for (int i = 0; i < parallelism; i++)
            {
                histograms[i] = ArrayPool<int>.Shared.Rent(BucketCount);
            }

            try
            {
                // Ranges
                var ranges = new Tuple<int, int>[parallelism];
                int chunkSize = length / parallelism;
                int remainder = length % parallelism;
                int currentStart = 0;

                for (int i = 0; i < parallelism; i++)
                {
                    int end = currentStart + chunkSize + (i < remainder ? 1 : 0);
                    ranges[i] = Tuple.Create(currentStart, end);
                    currentStart = end;
                }

                // 3. Pässe (0, 11, 22 bit shift)
                ExecuteRadixPass(pHashes, pIndices, pTempIndices, histograms, ranges, 0);
                ExecuteRadixPass(pHashes, pTempIndices, pIndices, histograms, ranges, 11);
                ExecuteRadixPass(pHashes, pIndices, pTempIndices, histograms, ranges, 22);
            }
            finally
            {
                for (int i = 0; i < parallelism; i++)
                {
                    if (histograms[i] != null) ArrayPool<int>.Shared.Return(histograms[i]);
                }
            }
        }

        private static void InitializeIndicesSIMD(int* pIndices, int length)
        {
            if (Avx2.IsSupported)
            {
                int parallelism = Environment.ProcessorCount;
                int chunkSize = length / parallelism;

                Parallel.For(0, parallelism, p =>
                {
                    int start = p * chunkSize;
                    int end = (p == parallelism - 1) ? length : start + chunkSize;
                    int* ptr = pIndices + start;
                    int i = start;

                    Vector256<int> vIndex = Vector256.Create(i, i + 1, i + 2, i + 3, i + 4, i + 5, i + 6, i + 7);
                    Vector256<int> vIncrement = Vector256.Create(8);
                    int simdEnd = start + ((end - start) & ~7);

                    while (i < simdEnd)
                    {
                        Avx2.Store(ptr, vIndex);
                        vIndex = Avx2.Add(vIndex, vIncrement);
                        ptr += 8;
                        i += 8;
                    }
                    while (i < end) *ptr++ = i++;
                });
            }
            else
            {
                Parallel.For(0, length, i => pIndices[i] = i);
            }
        }

        private static void ExecuteRadixPass(
            int* pHashes,
            int* pSourceIndices,
            int* pDestIndices,
            int[][] histograms,
            Tuple<int, int>[] ranges,
            int shift)
        {
            int parallelism = histograms.Length;

            // 1. Histogram
            Parallel.For(0, parallelism, p =>
            {
                int[] localCounts = histograms[p];
                Array.Clear(localCounts, 0, BucketCount);
                int start = ranges[p].Item1;
                int end = ranges[p].Item2;

                fixed (int* pCounts = localCounts)
                {
                    for (int i = start; i < end; i++)
                    {
                        int idx = pSourceIndices[i];
                        int hash = pHashes[idx];
                        int bucket = (int)((uint)hash >> shift) & Mask;
                        pCounts[bucket]++;
                    }
                }
            });

            // 2. Prefix Sum
            int globalOffset = 0;
            for (int b = 0; b < BucketCount; b++)
            {
                for (int p = 0; p < parallelism; p++)
                {
                    int count = histograms[p][b];
                    histograms[p][b] = globalOffset;
                    globalOffset += count;
                }
            }

            // 3. Scatter
            Parallel.For(0, parallelism, p =>
            {
                int[] localOffsets = histograms[p];
                int start = ranges[p].Item1;
                int end = ranges[p].Item2;

                fixed (int* pOffsets = localOffsets)
                {
                    for (int i = start; i < end; i++)
                    {
                        int idx = pSourceIndices[i];
                        int hash = pHashes[idx];
                        int bucket = (int)((uint)hash >> shift) & Mask;
                        int destPos = pOffsets[bucket]++;
                        pDestIndices[destPos] = idx;
                    }
                }
            });
        }
    }
}