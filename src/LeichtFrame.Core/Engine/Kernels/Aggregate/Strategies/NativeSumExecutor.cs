using LeichtFrame.Core.Engine;

namespace LeichtFrame.Core.Engine.Kernels.Aggregate.Strategies
{
    internal static unsafe class NativeSumExecutor
    {
        public static DataFrame Execute(GroupedDataFrame gdf, NativeGroupedData native, IntColumn ic, string colName)
        {
            var (startIdx, validGroups, totalRows, hasNulls) = NativeAggregationUtils.GetMetadata(gdf, native);

            var sumCol = new DoubleColumn($"Sum_{colName}", totalRows);
            IColumn keyCol = NativeAggregationUtils.CreateKeyColumn(gdf, native, totalRows, hasNulls, startIdx, validGroups);

            int* pOffsets = native.Offsets.Ptr;
            int* pIndices = native.Indices.Ptr;

            fixed (int* pSource = ic.Values.Span)
            {
                for (int i = 0; i < validGroups; i++)
                {
                    int nativeIndex = startIdx + i;
                    int start = pOffsets[nativeIndex];
                    int end = pOffsets[nativeIndex + 1];
                    long sum = 0;
                    for (int k = start; k < end; k++) sum += pSource[pIndices[k]];
                    sumCol.Append(sum);
                }
            }

            NativeAggregationUtils.AppendNullsIfNecessary(gdf, native, hasNulls, startIdx, keyCol, sumCol, ic);
            return new DataFrame(new IColumn[] { keyCol, sumCol });
        }
    }
}