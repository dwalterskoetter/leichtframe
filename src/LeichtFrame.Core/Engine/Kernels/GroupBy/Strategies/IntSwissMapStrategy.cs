using System.Runtime.InteropServices;
using LeichtFrame.Core.Engine.Collections;

namespace LeichtFrame.Core.Engine.Kernels.GroupBy.Strategies
{
    internal class IntSwissMapStrategy : IGroupByStrategy
    {
        public GroupedDataFrame Group(DataFrame df, string columnName)
        {
            var col = (IntColumn)df[columnName];
            return GroupNative(df, columnName, col);
        }

        private unsafe GroupedDataFrame GroupNative(DataFrame df, string colName, IntColumn col)
        {
            int rowCount = df.RowCount;
            if (rowCount == 0)
                return new GroupedDataFrame<int>(df, new[] { colName }, Array.Empty<int>(), new[] { 0 }, Array.Empty<int>(), null);

            int* pRowToGroupId = (int*)NativeMemory.Alloc((nuint)(rowCount * sizeof(int)));

            NativeIntMap map = new NativeIntMap(Math.Max(1024, rowCount / 10));

            List<int>? nullIndices = null;
            bool isNullable = col.IsNullable;

            try
            {
                // =========================================================
                // PHASE 1: Build Map & Assign IDs
                // =========================================================
                ReadOnlySpan<int> data = col.Values.Span;
                fixed (int* pData = data)
                {
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
                            pRowToGroupId[i] = map.GetOrAdd(pData[i]);
                        }
                    }
                    else
                    {
                        for (int i = 0; i < rowCount; i++)
                        {
                            pRowToGroupId[i] = map.GetOrAdd(pData[i]);
                        }
                    }
                }

                int groupCount = map.Count;

                // =========================================================
                // PHASE 2: Build CSR
                // =========================================================

                var resultNative = new NativeGroupedData(rowCount, groupCount);

                map.ExportKeysTo(resultNative.Keys.Ptr);

                int* pOffsets = resultNative.Offsets.Ptr;
                int* pIndices = resultNative.Indices.Ptr;

                new Span<int>(pOffsets, groupCount + 1).Fill(0);

                for (int i = 0; i < rowCount; i++)
                {
                    int gid = pRowToGroupId[i];
                    if (gid != -1)
                    {
                        pOffsets[gid]++;
                    }
                }

                int currentOffset = 0;
                int* pWriteHeads = (int*)NativeMemory.Alloc((nuint)(groupCount * sizeof(int)));

                for (int i = 0; i < groupCount; i++)
                {
                    int count = pOffsets[i];
                    pOffsets[i] = currentOffset;
                    pWriteHeads[i] = currentOffset;
                    currentOffset += count;
                }
                pOffsets[groupCount] = currentOffset;

                for (int i = 0; i < rowCount; i++)
                {
                    int gid = pRowToGroupId[i];
                    if (gid != -1)
                    {
                        int dest = pWriteHeads[gid];
                        pIndices[dest] = i;
                        pWriteHeads[gid]++;
                    }
                }

                NativeMemory.Free(pWriteHeads);

                return new GroupedDataFrame<int>(
                    df,
                    new[] { colName },
                    resultNative,
                    (nullIndices != null && nullIndices.Count > 0) ? nullIndices.ToArray() : null
                );
            }
            finally
            {
                map.Dispose();
                NativeMemory.Free(pRowToGroupId);
            }
        }
    }
}