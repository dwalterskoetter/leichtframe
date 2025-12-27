using System.Runtime.InteropServices;
using LeichtFrame.Core.Engine.Algorithms.Partitioning;
using LeichtFrame.Core.Engine.Collections;

namespace LeichtFrame.Core.Engine.Kernels.GroupBy.Strategies
{
    internal static unsafe class ParallelSwissMapExecutor
    {
        private const int MinRowsForParallel = 500_000;

        // ---------------------------------------------------------
        // STRING VARIANT
        // ---------------------------------------------------------
        public static NativeGroupedData? TryExecuteString(
            byte* pBytes, int* pOffsets, int* pHashes, int rowCount)
        {
            if (rowCount < MinRowsForParallel) return null;

            int partitionCount = DeterminePartitionCount(rowCount);
            int shift = 32 - System.Numerics.BitOperations.Log2((uint)partitionCount);
            int[] partitionOffsets = new int[partitionCount + 1];

            // 1. Partitionierung (Shuffle) - Zero Alloc
            RadixPartitioner.Partition(
                pHashes, rowCount, partitionCount, shift,
                out int* pPartHashes, out int* pPartRowIndices, partitionOffsets
            );

            try
            {
                // 2. Parallel Build (Independent Maps)
                var partitionResults = new NativeGroupedData[partitionCount];

                Parallel.For(0, partitionCount, p =>
                {
                    int start = partitionOffsets[p];
                    int end = partitionOffsets[p + 1];
                    int len = end - start;

                    if (len == 0)
                    {
                        partitionResults[p] = new NativeGroupedData(0, 0);
                        return;
                    }

                    var map = new NativeStringMap(Math.Max(64, len), pBytes, pOffsets);
                    int* pLocalGroupIds = (int*)NativeMemory.Alloc((nuint)(len * sizeof(int)));

                    try
                    {
                        for (int i = 0; i < len; i++)
                        {
                            int globalRowIdx = pPartRowIndices[start + i];
                            int hash = pPartHashes[start + i];

                            pLocalGroupIds[i] = map.GetOrAdd(globalRowIdx, hash);
                        }

                        int groupCount = map.Count;
                        var localRes = new NativeGroupedData(len, groupCount);
                        map.ExportRowIndicesTo(localRes.Keys.Ptr);
                        BuildCsr(pLocalGroupIds, localRes, len, groupCount);
                        partitionResults[p] = localRes;
                    }
                    finally
                    {
                        map.Dispose();
                        NativeMemory.Free(pLocalGroupIds);
                    }
                });

                // 3. Merge Results
                return MergeResults(partitionResults, pPartRowIndices, partitionOffsets, rowCount);
            }
            finally
            {
                NativeMemory.Free(pPartHashes);
                NativeMemory.Free(pPartRowIndices);
            }
        }

        // ---------------------------------------------------------
        // HELPERS
        // ---------------------------------------------------------

        private static void BuildCsr(int* groupIds, NativeGroupedData res, int len, int groupCount)
        {
            int* pOffsets = res.Offsets.Ptr;
            int* pIndices = res.Indices.Ptr;

            new Span<int>(pOffsets, groupCount + 1).Fill(0);

            // Histogram
            for (int i = 0; i < len; i++) pOffsets[groupIds[i]]++;

            // Prefix Sum & Temp Write Heads
            int current = 0;
            int* writeHeads = (int*)NativeMemory.Alloc((nuint)(groupCount * 4));
            for (int i = 0; i < groupCount; i++)
            {
                int c = pOffsets[i];
                pOffsets[i] = current;
                writeHeads[i] = current;
                current += c;
            }
            pOffsets[groupCount] = current;

            // Scatter
            for (int i = 0; i < len; i++)
            {
                int gid = groupIds[i];
                pIndices[writeHeads[gid]++] = i;
            }
            NativeMemory.Free(writeHeads);
        }

        private static NativeGroupedData MergeResults(
            NativeGroupedData[] parts,
            int* pPartRowIndices,
            int[] partitionOffsets,
            int totalRows)
        {
            int totalGroups = 0;
            foreach (var p in parts) totalGroups += p.GroupCount;

            var result = new NativeGroupedData(totalRows, totalGroups);

            int globalGroupOffset = 0;
            int globalIndexOffset = 0;

            result.Offsets.Ptr[0] = 0;

            for (int p = 0; p < parts.Length; p++)
            {
                var part = parts[p];
                int count = part.GroupCount;
                if (count == 0) { part.Dispose(); continue; }

                nuint byteCountKeys = (nuint)(count * sizeof(int));
                NativeMemory.Copy(part.Keys.Ptr, result.Keys.Ptr + globalGroupOffset, byteCountKeys);

                int* pLocalOffsets = part.Offsets.Ptr;
                int* pGlobalOffsets = result.Offsets.Ptr + globalGroupOffset;

                for (int i = 0; i < count; i++)
                {
                    int groupSize = pLocalOffsets[i + 1] - pLocalOffsets[i];
                    globalIndexOffset += groupSize;
                    pGlobalOffsets[i + 1] = globalIndexOffset;
                }

                int partStartInGlobalArray = partitionOffsets[p];
                int partRowCount = part.RowCount;

                int writeStart = result.Offsets.Ptr[globalGroupOffset];

                int* pLocalIndices = part.Indices.Ptr;
                int* pGlobalIndices = result.Indices.Ptr;

                for (int i = 0; i < partRowCount; i++)
                {
                    int localIdx = pLocalIndices[i];
                    int globalRowIdx = pPartRowIndices[partStartInGlobalArray + localIdx];
                    pGlobalIndices[writeStart + i] = globalRowIdx;
                }

                globalGroupOffset += count;
                part.Dispose();
            }

            return result;
        }

        private static int DeterminePartitionCount(int rowCount)
        {
            int targetSize = 100_000;
            int parts = rowCount / targetSize;

            if (parts < 16) return 16;
            if (parts > 1024) return 1024;
            return (int)System.Numerics.BitOperations.RoundUpToPowerOf2((uint)parts);
        }
    }
}