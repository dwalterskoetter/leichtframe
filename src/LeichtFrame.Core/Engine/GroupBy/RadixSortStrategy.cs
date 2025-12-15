using System.Buffers;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;

namespace LeichtFrame.Core
{
    internal class RadixSortStrategy : IGroupByStrategy
    {
        private const int RadixBits = 16;
        private const int BucketCount = 1 << RadixBits; // 65,536
        private const int Mask = BucketCount - 1;

        public GroupedDataFrame Group(DataFrame df, string columnName)
        {
            var col = df[columnName];
            if (col is IntColumn intCol)
            {
                return GroupInt32(df, columnName, intCol);
            }
            throw new NotSupportedException($"God Mode currently supports [Int32] only.");
        }

        private unsafe GroupedDataFrame GroupInt32(DataFrame df, string columnName, IntColumn col)
        {
            int rowCount = df.RowCount;
            if (rowCount == 0) return BuildCsr(df, columnName, col, null, null, 0);

            int parallelism = Environment.ProcessorCount;

            // 1. Unmanaged Buffers (Zero GC Pressure for Data)
            using var hashesBuffer = new UnsafeBuffer<int>(rowCount);
            using var indicesBuffer = new UnsafeBuffer<int>(rowCount);
            using var tempIndicesBuffer = new UnsafeBuffer<int>(rowCount);

            int* pHashes = hashesBuffer.Ptr;
            int* pIndices = indicesBuffer.Ptr;
            int* pTempIndices = tempIndicesBuffer.Ptr;

            ReadOnlyMemory<int> keysMemory = col.Values;
            int rangeSize = Math.Max(1, rowCount / parallelism);
            var rangePartitioner = Partitioner.Create(0, rowCount, rangeSize);

            // 2. Parallel Hash & Init
            Parallel.ForEach(rangePartitioner, range =>
            {
                ReadOnlySpan<int> keysSpan = keysMemory.Span;
                for (int i = range.Item1; i < range.Item2; i++) pIndices[i] = i;

                var sliceKeys = keysSpan.Slice(range.Item1, range.Item2 - range.Item1);
                var sliceHashes = new Span<int>(pHashes + range.Item1, range.Item2 - range.Item1);
                VectorizedHasher.HashIntegers(sliceKeys, sliceHashes);
            });

            // 3. Parallel Radix Sort (Using ArrayPool Histograms for JIT Speed)
            ParallelRadixSort(pHashes, pIndices, pTempIndices, rowCount, parallelism);

            // 4. Build CSR (Pointer optimized)
            return BuildCsr(df, columnName, col, pHashes, pIndices, rowCount);
        }

        private unsafe void ParallelRadixSort(int* pHashes, int* pIndices, int* pTempIndices, int length, int parallelism)
        {
            // Hybrid: Unmanaged Data + Managed Histograms (Faster JIT)
            int[][] histograms = new int[parallelism][];
            for (int i = 0; i < parallelism; i++)
            {
                histograms[i] = ArrayPool<int>.Shared.Rent(BucketCount);
            }

            try
            {
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

                // Pass 1
                ExecuteRadixPass(pHashes, pIndices, pTempIndices, histograms, ranges, 0);
                // Pass 2
                ExecuteRadixPass(pHashes, pTempIndices, pIndices, histograms, ranges, 16);
            }
            finally
            {
                for (int i = 0; i < parallelism; i++)
                {
                    if (histograms[i] != null) ArrayPool<int>.Shared.Return(histograms[i]);
                }
            }
        }

        private unsafe void ExecuteRadixPass(
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

                // Fast Managed Array Access
                for (int i = start; i < end; i++)
                {
                    int idx = pSourceIndices[i];
                    int hash = pHashes[idx];
                    int bucket = (int)((uint)hash >> shift) & Mask;
                    localCounts[bucket]++;
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

                for (int i = start; i < end; i++)
                {
                    int idx = pSourceIndices[i];
                    int hash = pHashes[idx];
                    int bucket = (int)((uint)hash >> shift) & Mask;

                    int destPos = localOffsets[bucket]++;
                    pDestIndices[destPos] = idx;
                }
            });
        }

        private unsafe GroupedDataFrame BuildCsr(
            DataFrame df,
            string columnName,
            IntColumn col,
            int* pHashes,
            int* pIndices,
            int length)
        {
            if (length == 0) return new GroupedDataFrame<int>(df, columnName, Array.Empty<int>(), new[] { 0 }, Array.Empty<int>(), null);

            int[] keys = new int[length];
            int[] offsets = new int[length + 1];
            int[] finalIndices = new int[length];

            // Bulk copy unmanaged indices to managed array (fastest way)
            Marshal.Copy((nint)pIndices, finalIndices, 0, length);

            int groupCount = 0;

            // Pointer optimization for Result Arrays to eliminate Bounds Checks
            fixed (int* pKeys = keys)
            fixed (int* pOffsets = offsets)
            {
                pOffsets[0] = 0;

                var values = col.Values.Span;
                int prevIdx = pIndices[0];
                int prevVal = values[prevIdx];
                int prevHash = pHashes[prevIdx];

                for (int i = 1; i < length; i++)
                {
                    int currIdx = pIndices[i];
                    int currHash = pHashes[currIdx];

                    bool isDiff = false;

                    if (currHash != prevHash)
                    {
                        isDiff = true;
                    }
                    else
                    {
                        int currVal = values[currIdx];
                        if (currVal != prevVal)
                        {
                            isDiff = true;
                            prevVal = currVal;
                        }
                    }

                    if (isDiff)
                    {
                        pKeys[groupCount] = prevVal; // Unsafe write
                        groupCount++;
                        pOffsets[groupCount] = i;    // Unsafe write

                        prevHash = currHash;
                        prevVal = values[currIdx];
                    }
                }

                pKeys[groupCount] = prevVal;
                groupCount++;
                pOffsets[groupCount] = length;
            }

            if (groupCount < length)
            {
                Array.Resize(ref keys, groupCount);
                Array.Resize(ref offsets, groupCount + 1);
            }

            return new GroupedDataFrame<int>(df, columnName, keys, offsets, finalIndices, null);
        }
    }
}