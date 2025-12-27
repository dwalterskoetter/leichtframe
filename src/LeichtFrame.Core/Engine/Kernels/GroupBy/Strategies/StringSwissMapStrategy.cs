using System.Runtime.InteropServices;
using LeichtFrame.Core.Engine.Collections;

namespace LeichtFrame.Core.Engine.Kernels.GroupBy.Strategies
{
    internal class StringSwissMapStrategy : IGroupByStrategy
    {
        public GroupedDataFrame Group(DataFrame df, string columnName)
        {
            var col = (StringColumn)df[columnName];
            return GroupNative(df, columnName, col);
        }

        private unsafe GroupedDataFrame GroupNative(DataFrame df, string colName, StringColumn col)
        {
            int rowCount = df.RowCount;
            if (rowCount == 0)
                return new GroupedDataFrame<string>(df, new[] { colName }, Array.Empty<string>(), new[] { 0 }, Array.Empty<int>(), null);

            int* pHashes = (int*)NativeMemory.Alloc((nuint)(rowCount * sizeof(int)));

            fixed (byte* pBytes = col.RawBytes)
            fixed (int* pOffsets = col.Offsets)
            {
                // 1. Vectorized Hashing (Zero Alloc)
                VectorizedHasher.HashStrings(pBytes, pOffsets, pHashes, rowCount);

                if (!col.IsNullable)
                {
                    var parallelRes = ParallelSwissMapExecutor.TryExecuteString(pBytes, pOffsets, pHashes, rowCount);
                    if (parallelRes != null)
                    {
                        var gdf = new GroupedDataFrame<string>(df, new[] { colName }, parallelRes, null);
                        gdf.KeysAreRowIndices = true;
                        NativeMemory.Free(pHashes);
                        return gdf;
                    }
                }

                int* pRowToGroupId = (int*)NativeMemory.Alloc((nuint)(rowCount * sizeof(int)));

                List<int>? nullIndices = null;
                bool isNullable = col.IsNullable;

                var map = new NativeStringMap(Math.Max(1024, rowCount / 10), pBytes, pOffsets);

                try
                {
                    // -- PHASE 1: INSERT --
                    if (isNullable)
                    {
                        for (int i = 0; i < rowCount; i++)
                        {
                            if (col.IsNull(i))
                            {
                                if (nullIndices == null) nullIndices = new List<int>();
                                nullIndices.Add(i);
                                pRowToGroupId[i] = -1;
                                continue;
                            }
                            pRowToGroupId[i] = map.GetOrAdd(i, pHashes[i]);
                        }
                    }
                    else
                    {
                        for (int i = 0; i < rowCount; i++)
                        {
                            pRowToGroupId[i] = map.GetOrAdd(i, pHashes[i]);
                        }
                    }

                    int groupCount = map.Count;

                    // -- PHASE 2: CSR --

                    var resultNative = new NativeGroupedData(rowCount, groupCount);
                    int* pOffsetsRes = resultNative.Offsets.Ptr;
                    int* pIndicesRes = resultNative.Indices.Ptr;
                    int* pKeysRes = resultNative.Keys.Ptr;

                    // A. Keys
                    map.ExportRowIndicesTo(pKeysRes);

                    // B. Histogramm
                    new Span<int>(pOffsetsRes, groupCount + 1).Fill(0);
                    for (int i = 0; i < rowCount; i++)
                    {
                        int gid = pRowToGroupId[i];
                        if (gid != -1) pOffsetsRes[gid]++;
                    }

                    // C. Prefix Sum
                    int currentOffset = 0;
                    int* pWriteHeads = (int*)NativeMemory.Alloc((nuint)(groupCount * sizeof(int)));
                    for (int i = 0; i < groupCount; i++)
                    {
                        int c = pOffsetsRes[i];
                        pOffsetsRes[i] = currentOffset;
                        pWriteHeads[i] = currentOffset;
                        currentOffset += c;
                    }
                    pOffsetsRes[groupCount] = currentOffset;

                    // D. Scatter
                    for (int i = 0; i < rowCount; i++)
                    {
                        int gid = pRowToGroupId[i];
                        if (gid != -1)
                        {
                            int dest = pWriteHeads[gid];
                            pIndicesRes[dest] = i;
                            pWriteHeads[gid]++;
                        }
                    }
                    NativeMemory.Free(pWriteHeads);

                    var gdf = new GroupedDataFrame<string>(
                        df,
                        new[] { colName },
                        resultNative,
                        (nullIndices != null && nullIndices.Count > 0) ? nullIndices.ToArray() : null
                    );

                    gdf.KeysAreRowIndices = true;

                    return gdf;
                }
                finally
                {
                    map.Dispose();
                    NativeMemory.Free(pRowToGroupId);
                    NativeMemory.Free(pHashes);
                }
            }
        }
    }
}