using System.Runtime.InteropServices;
using LeichtFrame.Core.Engine.Collections;
using LeichtFrame.Core.Engine.Algorithms.Packing;

namespace LeichtFrame.Core.Engine.Kernels.GroupBy.Strategies
{
    internal class RowLayoutHashStrategy : IGroupByStrategy
    {
        public GroupedDataFrame Group(DataFrame df, string ignored) => throw new NotSupportedException();

        public unsafe GroupedDataFrame Group(DataFrame df, string[] cols)
        {
            var packed = RowLayoutPacking.Pack(df, cols);
            byte* pRows = (byte*)packed.Buffer;
            int width = packed.Width;
            int rowCount = df.RowCount;

            int* pHashes = (int*)NativeMemory.Alloc((nuint)(rowCount * sizeof(int)));

            Parallel.For(0, rowCount, i =>
            {
                byte* pRow = pRows + (i * width);

                int h = unchecked((int)2166136261);

                for (int b = 0; b < width; b++)
                {
                    h = (h ^ pRow[b]) * 16777619;
                }

                pHashes[i] = h;
            });

            var map = new NativeRowMap(Math.Max(1024, rowCount / 10), pRows, width);
            int* pRowToGroupId = (int*)NativeMemory.Alloc((nuint)(rowCount * sizeof(int)));

            try
            {
                for (int i = 0; i < rowCount; i++)
                {
                    pRowToGroupId[i] = map.GetOrAdd(i, pHashes[i]);
                }

                int groupCount = map.Count;
                var resultNative = new NativeGroupedData(rowCount, groupCount);
                int* pOffsets = resultNative.Offsets.Ptr;
                int* pIndices = resultNative.Indices.Ptr;
                int* pKeys = resultNative.Keys.Ptr;

                int[] repRowIndices = map.ExportKeysAsRowIndices();
                Marshal.Copy(repRowIndices, 0, (nint)pKeys, groupCount);

                new Span<int>(pOffsets, groupCount + 1).Fill(0);
                for (int i = 0; i < rowCount; i++) pOffsets[pRowToGroupId[i]]++;

                int current = 0;
                int* pWrite = (int*)NativeMemory.Alloc((nuint)(groupCount * 4));
                for (int i = 0; i < groupCount; i++)
                {
                    int c = pOffsets[i];
                    pOffsets[i] = current;
                    pWrite[i] = current;
                    current += c;
                }
                pOffsets[groupCount] = current;

                for (int i = 0; i < rowCount; i++)
                {
                    int gid = pRowToGroupId[i];
                    pIndices[pWrite[gid]++] = i;
                }
                NativeMemory.Free(pWrite);

                var gdf = new GroupedDataFrame<int>(df, cols, resultNative, null);
                gdf.KeysAreRowIndices = true;
                return gdf;
            }
            finally
            {
                map.Dispose();
                NativeMemory.Free(pHashes);
                NativeMemory.Free(pRowToGroupId);
                NativeMemory.Free(pRows);
            }
        }
    }
}