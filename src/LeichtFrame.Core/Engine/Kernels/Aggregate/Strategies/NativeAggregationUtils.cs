namespace LeichtFrame.Core.Engine.Kernels.Aggregate
{
    internal static unsafe class NativeAggregationUtils
    {
        public static (int startIdx, int validGroups, int totalRows, bool hasNulls) GetMetadata(GroupedDataFrame gdf, NativeGroupedData native)
        {
            int startIdx = 0;
            if (gdf is DictionaryGroupedDataFrame d && native.GroupCount > 0)
            {
                startIdx = d.NativeStartOffset;
            }

            int validGroups = native.GroupCount - startIdx;
            if (validGroups < 0) validGroups = 0;

            bool hasNulls = (gdf.NullGroupIndices != null && gdf.NullGroupIndices.Length > 0) || startIdx == 1;

            int totalRows = validGroups + (hasNulls ? 1 : 0);
            return (startIdx, validGroups, totalRows, hasNulls);
        }

        public static IColumn CreateKeyColumn(GroupedDataFrame gdf, NativeGroupedData native, int totalRows, bool hasNulls, int startIdx, int validGroups)
        {
            if (gdf is DictionaryGroupedDataFrame dictGdf)
            {
                var strCol = new StringColumn(gdf.GroupColumnNames[0], totalRows, isNullable: hasNulls);
                var dict = dictGdf.InternalDictionary;
                int* pKeys = native.Keys.Ptr;
                for (int i = 0; i < validGroups; i++)
                {
                    strCol.Append(dict[pKeys[startIdx + i]]);
                }
                return strCol;
            }
            else
            {
                var intCol = new IntColumn(gdf.GroupColumnNames[0], totalRows, isNullable: hasNulls);
                int* pKeys = native.Keys.Ptr;
                for (int i = 0; i < validGroups; i++)
                {
                    intCol.Append(pKeys[startIdx + i]);
                }
                return intCol;
            }
        }

        public static void AppendNullsIfNecessary(
            GroupedDataFrame gdf,
            NativeGroupedData native,
            bool hasNulls,
            int startIdx,
            IColumn keyCol,
            IColumn resCol,
            IntColumn? valSource)
        {
            if (!hasNulls) return;

            keyCol.AppendObject(null);

            bool useManaged = gdf.NullGroupIndices != null && gdf.NullGroupIndices.Length > 0;
            bool useNative = !useManaged && startIdx == 1;

            // Handle Count (Int Result)
            if (resCol is IntColumn icRes && (valSource == null || resCol.Name.StartsWith("Count")))
            {
                int count = 0;
                if (useManaged) count = gdf.NullGroupIndices!.Length;
                else if (useNative) count = native.Offsets.Ptr[1] - native.Offsets.Ptr[0];

                icRes.Append(count);
                return;
            }

            // Handle Sum/Min/Max (Logic requiring Source)
            if (valSource != null)
            {
                var span = valSource.Values.Span;

                // Sum (Double Result)
                if (resCol is DoubleColumn dcRes)
                {
                    long sum = 0;
                    if (useManaged)
                    {
                        foreach (var idx in gdf.NullGroupIndices!) sum += span[idx];
                    }
                    else if (useNative)
                    {
                        int end = native.Offsets.Ptr[1];
                        int* pIndices = native.Indices.Ptr;
                        for (int k = 0; k < end; k++) sum += span[pIndices[k]];
                    }
                    dcRes.Append(sum);
                }
                // Min/Max (Int Result)
                else if (resCol is IntColumn icResVal)
                {
                    bool isMin = resCol.Name.StartsWith("Min");
                    int val = isMin ? int.MaxValue : int.MinValue;
                    bool found = false;

                    if (useManaged)
                    {
                        found = true;
                        foreach (var idx in gdf.NullGroupIndices!)
                        {
                            int v = span[idx];
                            if (isMin) { if (v < val) val = v; } else { if (v > val) val = v; }
                        }
                    }
                    else if (useNative)
                    {
                        int end = native.Offsets.Ptr[1];
                        if (end > 0) found = true;
                        int* pIndices = native.Indices.Ptr;
                        for (int k = 0; k < end; k++)
                        {
                            int v = span[pIndices[k]];
                            if (isMin) { if (v < val) val = v; } else { if (v > val) val = v; }
                        }
                    }

                    if (!found) val = 0;
                    icResVal.Append(val);
                }
            }
        }
    }
}