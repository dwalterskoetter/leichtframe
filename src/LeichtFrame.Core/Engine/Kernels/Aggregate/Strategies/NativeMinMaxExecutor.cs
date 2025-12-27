using LeichtFrame.Core.Engine;

namespace LeichtFrame.Core.Engine.Kernels.Aggregate.Strategies
{
    internal static unsafe class NativeMinMaxExecutor
    {
        public static DataFrame Execute(GroupedDataFrame gdf, NativeGroupedData native, IntColumn ic, string colName, bool isMin)
        {
            var (startIdx, validGroups, totalRows, hasNulls) = NativeAggregationUtils.GetMetadata(gdf, native);

            // Min/Max on IntColumn returns IntColumn
            var resCol = new IntColumn($"{(isMin ? "Min" : "Max")}_{colName}", totalRows);
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

                    if (start == end) { resCol.Append(0); continue; }

                    int val = isMin ? int.MaxValue : int.MinValue;
                    for (int k = start; k < end; k++)
                    {
                        int v = pSource[pIndices[k]];
                        if (isMin) { if (v < val) val = v; } else { if (v > val) val = v; }
                    }
                    resCol.Append(val);
                }
            }

            NativeAggregationUtils.AppendNullsIfNecessary(gdf, native, hasNulls, startIdx, keyCol, resCol, ic);
            return new DataFrame(new IColumn[] { keyCol, resCol });
        }
    }
}