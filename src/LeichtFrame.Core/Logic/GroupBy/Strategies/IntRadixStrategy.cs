using System.Buffers;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using LeichtFrame.Core.Engine;

namespace LeichtFrame.Core.Logic
{
    /// <summary>
    /// A high-performance grouping strategy for High-Cardinality Integer columns.
    /// Orchestrates the data flow between the DataFrame and the SIMD Engine.
    /// </summary>
    internal class IntRadixStrategy : IGroupByStrategy
    {
        public GroupedDataFrame Group(DataFrame df, string columnName)
        {
            var col = (IntColumn)df[columnName];
            return GroupInt32(df, columnName, col);
        }

        private unsafe GroupedDataFrame GroupInt32(DataFrame df, string columnName, IntColumn col)
        {
            int rowCount = df.RowCount;

            if (rowCount == 0) return BuildCsr(df, columnName, col, null, null, 0);

            int parallelism = Environment.ProcessorCount;

            using var hashesBuffer = new UnsafeBuffer<int>(rowCount);
            using var indicesBuffer = new UnsafeBuffer<int>(rowCount);
            using var tempIndicesBuffer = new UnsafeBuffer<int>(rowCount);

            int* pHashes = hashesBuffer.Ptr;
            int* pIndices = indicesBuffer.Ptr;
            int* pTempIndices = tempIndicesBuffer.Ptr;

            ReadOnlyMemory<int> keysMemory = col.Values;
            int rangeSize = Math.Max(1, rowCount / parallelism);
            var rangePartitioner = Partitioner.Create(0, rowCount, rangeSize);

            Parallel.ForEach(rangePartitioner, range =>
            {
                ReadOnlySpan<int> keysSpan = keysMemory.Span;

                var sliceKeys = keysSpan.Slice(range.Item1, range.Item2 - range.Item1);
                var sliceHashes = new Span<int>(pHashes + range.Item1, range.Item2 - range.Item1);

                VectorizedHasher.HashIntegers(sliceKeys, sliceHashes);
            });

            SimdRadixSorter.Sort(pHashes, pIndices, pTempIndices, rowCount);

            return BuildCsr(df, columnName, col, pHashes, pIndices, rowCount);
        }

        /// <summary>
        /// Transforms the sorted indices and hashes into a CSR (Compressed Sparse Row) representation.
        /// </summary>
        private unsafe GroupedDataFrame BuildCsr(
            DataFrame df,
            string columnName,
            IntColumn col,
            int* pHashes,
            int* pIndices,
            int length)
        {
            if (length == 0)
                return new GroupedDataFrame<int>(df, columnName, Array.Empty<int>(), new[] { 0 }, Array.Empty<int>(), null);

            int[] tempKeys = ArrayPool<int>.Shared.Rent(length);
            int[] tempOffsets = ArrayPool<int>.Shared.Rent(length + 1);

            int[] finalIndices = new int[length];

            Marshal.Copy((nint)pIndices, finalIndices, 0, length);

            int groupCount = 0;

            try
            {
                fixed (int* pKeys = tempKeys)
                fixed (int* pOffsets = tempOffsets)
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
                            pKeys[groupCount] = prevVal;
                            groupCount++;
                            pOffsets[groupCount] = i;

                            prevHash = currHash;
                            prevVal = values[currIdx];
                        }
                    }

                    pKeys[groupCount] = prevVal;
                    groupCount++;
                    pOffsets[groupCount] = length;
                }

                int[] finalKeys = new int[groupCount];
                Array.Copy(tempKeys, finalKeys, groupCount);

                int[] finalOffsets = new int[groupCount + 1];
                Array.Copy(tempOffsets, finalOffsets, groupCount + 1);

                return new GroupedDataFrame<int>(df, columnName, finalKeys, finalOffsets, finalIndices, null);
            }
            finally
            {
                ArrayPool<int>.Shared.Return(tempKeys);
                ArrayPool<int>.Shared.Return(tempOffsets);
            }
        }
    }
}