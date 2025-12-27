using LeichtFrame.Core.Engine;

namespace LeichtFrame.Core.Engine.Kernels.Aggregate.Strategies
{
    internal static unsafe class NativeCountExecutor
    {
        public static DataFrame Execute(GroupedDataFrame gdf, NativeGroupedData native)
        {
            var (startIdx, validGroups, totalRows, hasNulls) = NativeAggregationUtils.GetMetadata(gdf, native);

            var nativeCounts = new IntColumn("Count", totalRows);
            IColumn keyCol = NativeAggregationUtils.CreateKeyColumn(gdf, native, totalRows, hasNulls, startIdx, validGroups);

            int* offsets = native.Offsets.Ptr;
            for (int i = 0; i < validGroups; i++)
            {
                int nativeIndex = startIdx + i;
                nativeCounts.Append(offsets[nativeIndex + 1] - offsets[nativeIndex]);
            }

            NativeAggregationUtils.AppendNullsIfNecessary(gdf, native, hasNulls, startIdx, keyCol, nativeCounts, null);

            return new DataFrame(new IColumn[] { keyCol, nativeCounts });
        }
    }
}