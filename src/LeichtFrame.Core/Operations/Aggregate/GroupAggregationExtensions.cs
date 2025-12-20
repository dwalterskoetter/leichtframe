namespace LeichtFrame.Core.Operations.Aggregate
{
    /// <summary>
    /// Provides extension methods for performing aggregations on grouped dataframes.
    /// Optimized for high performance using CSR (Compressed Sparse Row) iteration via pointers (Native Memory).
    /// </summary>
    public static class GroupAggregationExtensions
    {
        // --- Custom Delegates to support ReadOnlySpan (Managed Fallback) ---
        private delegate long IntSpanNullOp(ReadOnlySpan<int> data, int[] indices);
        private delegate long IntSpanCsrOp(ReadOnlySpan<int> data, int start, int end, int[] indices);
        private delegate double DoubleSpanCsrOp(ReadOnlySpan<double> data, int start, int end, int[] indices);

        /// <summary>
        /// Aggregates the grouped data by counting rows in each group.
        /// Returns a new DataFrame with columns: [GroupColumn, "Count"].
        /// </summary>
        public static DataFrame Count(this GroupedDataFrame gdf)
        {
            // --- FAST PATH: Native Memory (Zero-Alloc) ---
            if (gdf.NativeData != null)
            {
                var native = gdf.NativeData;
                var nativeCounts = new IntColumn("Count", native.GroupCount);

                unsafe
                {
                    int* offsets = native.Offsets.Ptr;
                    int* keys = native.Keys.Ptr;

                    for (int i = 0; i < native.GroupCount; i++)
                    {
                        nativeCounts.Append(offsets[i + 1] - offsets[i]);
                    }

                    // FIX: Use the first column name from the array
                    var keyCol = new IntColumn(gdf.GroupColumnNames[0], native.GroupCount);
                    for (int i = 0; i < native.GroupCount; i++) keyCol.Append(keys[i]);

                    return new DataFrame(new IColumn[] { keyCol, nativeCounts });
                }
            }

            // --- SLOW PATH: Managed Arrays (Fallback) ---
            var offsetsManaged = gdf.GroupOffsets;
            var count = gdf.GroupCount;
            bool hasNulls = gdf.NullGroupIndices != null;

            var countsCol = new IntColumn("Count", count + (hasNulls ? 1 : 0));

            for (int i = 0; i < count; i++)
            {
                countsCol.Append(offsetsManaged[i + 1] - offsetsManaged[i]);
            }

            if (hasNulls)
            {
                countsCol.Append(gdf.NullGroupIndices!.Length);
            }

            return CreateResultDataFrame(gdf, "Count", countsCol);
        }

        /// <summary>
        /// Aggregates the grouped data by summing values in the specified column.
        /// </summary>
        public static DataFrame Sum(this GroupedDataFrame gdf, string aggregateColumnName)
        {
            // --- FAST PATH: Native Memory ---
            if (gdf.NativeData != null)
            {
                var col = gdf.Source[aggregateColumnName];

                if (col is IntColumn ic)
                {
                    var native = gdf.NativeData;
                    var sumCol = new DoubleColumn($"Sum_{aggregateColumnName}", native.GroupCount);

                    unsafe
                    {
                        int* pOffsets = native.Offsets.Ptr;
                        int* pIndices = native.Indices.Ptr;
                        int* pKeys = native.Keys.Ptr;

                        fixed (int* pSource = ic.Values.Span)
                        {
                            for (int i = 0; i < native.GroupCount; i++)
                            {
                                int start = pOffsets[i];
                                int end = pOffsets[i + 1];

                                long sum = 0;
                                for (int k = start; k < end; k++)
                                {
                                    sum += pSource[pIndices[k]];
                                }
                                sumCol.Append(sum);
                            }
                        }

                        // FIX: Use the first column name from the array
                        var keyCol = new IntColumn(gdf.GroupColumnNames[0], native.GroupCount);
                        for (int i = 0; i < native.GroupCount; i++) keyCol.Append(pKeys[i]);

                        return new DataFrame(new IColumn[] { keyCol, sumCol });
                    }
                }
            }

            // --- SLOW PATH ---
            return ExecuteNumericAgg(gdf, aggregateColumnName, "Sum",
                (span, indices) =>
                {
                    long sum = 0;
                    foreach (var idx in indices) sum += span[idx];
                    return sum;
                },
                (span, start, end, indices) =>
                {
                    long sum = 0;
                    for (int k = start; k < end; k++) sum += span[indices[k]];
                    return sum;
                },
                (span, start, end, indices) =>
                {
                    double sum = 0;
                    for (int k = start; k < end; k++) sum += span[indices[k]];
                    return sum;
                }
            );
        }

        /// <summary>
        /// Aggregates the grouped data by finding the minimum value.
        /// </summary>
        public static DataFrame Min(this GroupedDataFrame gdf, string aggregateColumnName)
        {
            if (gdf.NativeData != null)
            {
                var col = gdf.Source[aggregateColumnName];
                if (col is IntColumn ic)
                {
                    var native = gdf.NativeData;
                    var minCol = new IntColumn($"Min_{aggregateColumnName}", native.GroupCount);

                    unsafe
                    {
                        int* pOffsets = native.Offsets.Ptr;
                        int* pIndices = native.Indices.Ptr;
                        int* pKeys = native.Keys.Ptr;

                        fixed (int* pSource = ic.Values.Span)
                        {
                            for (int i = 0; i < native.GroupCount; i++)
                            {
                                int start = pOffsets[i];
                                int end = pOffsets[i + 1];

                                if (start == end) { minCol.Append(0); continue; }

                                int min = int.MaxValue;
                                for (int k = start; k < end; k++)
                                {
                                    int val = pSource[pIndices[k]];
                                    if (val < min) min = val;
                                }
                                minCol.Append(min);
                            }
                        }
                        // FIX: Use the first column name
                        var keyCol = new IntColumn(gdf.GroupColumnNames[0], native.GroupCount);
                        for (int i = 0; i < native.GroupCount; i++) keyCol.Append(pKeys[i]);
                        return new DataFrame(new IColumn[] { keyCol, minCol });
                    }
                }
            }

            // --- SLOW PATH ---
            return ExecuteNumericAgg(gdf, aggregateColumnName, "Min",
                (span, indices) =>
                {
                    if (indices.Length == 0) return 0;
                    int min = int.MaxValue;
                    foreach (var idx in indices) if (span[idx] < min) min = span[idx];
                    return min;
                },
                (span, start, end, indices) =>
                {
                    if (start == end) return 0;
                    int min = int.MaxValue;
                    for (int k = start; k < end; k++)
                    {
                        int val = span[indices[k]];
                        if (val < min) min = val;
                    }
                    return min;
                },
                (span, start, end, indices) =>
                {
                    if (start == end) return 0;
                    double min = double.MaxValue;
                    for (int k = start; k < end; k++)
                    {
                        double val = span[indices[k]];
                        if (val < min) min = val;
                    }
                    return min;
                }
            );
        }

        /// <summary>
        /// Aggregates the grouped data by finding the maximum value.
        /// </summary>
        public static DataFrame Max(this GroupedDataFrame gdf, string aggregateColumnName)
        {
            if (gdf.NativeData != null)
            {
                var col = gdf.Source[aggregateColumnName];
                if (col is IntColumn ic)
                {
                    var native = gdf.NativeData;
                    var maxCol = new IntColumn($"Max_{aggregateColumnName}", native.GroupCount);

                    unsafe
                    {
                        int* pOffsets = native.Offsets.Ptr;
                        int* pIndices = native.Indices.Ptr;
                        int* pKeys = native.Keys.Ptr;

                        fixed (int* pSource = ic.Values.Span)
                        {
                            for (int i = 0; i < native.GroupCount; i++)
                            {
                                int start = pOffsets[i];
                                int end = pOffsets[i + 1];

                                if (start == end) { maxCol.Append(0); continue; }

                                int max = int.MinValue;
                                for (int k = start; k < end; k++)
                                {
                                    int val = pSource[pIndices[k]];
                                    if (val > max) max = val;
                                }
                                maxCol.Append(max);
                            }
                        }
                        // FIX: Use the first column name
                        var keyCol = new IntColumn(gdf.GroupColumnNames[0], native.GroupCount);
                        for (int i = 0; i < native.GroupCount; i++) keyCol.Append(pKeys[i]);
                        return new DataFrame(new IColumn[] { keyCol, maxCol });
                    }
                }
            }

            // --- SLOW PATH ---
            return ExecuteNumericAgg(gdf, aggregateColumnName, "Max",
                (span, indices) =>
                {
                    if (indices.Length == 0) return 0;
                    int max = int.MinValue;
                    foreach (var idx in indices) if (span[idx] > max) max = span[idx];
                    return max;
                },
                (span, start, end, indices) =>
                {
                    if (start == end) return 0;
                    int max = int.MinValue;
                    for (int k = start; k < end; k++)
                    {
                        int val = span[indices[k]];
                        if (val > max) max = val;
                    }
                    return max;
                },
                (span, start, end, indices) =>
                {
                    if (start == end) return 0;
                    double max = double.MinValue;
                    for (int k = start; k < end; k++)
                    {
                        double val = span[indices[k]];
                        if (val > max) max = val;
                    }
                    return max;
                }
            );
        }

        /// <summary>
        /// Aggregates the grouped data by calculating the mean value.
        /// </summary>
        public static DataFrame Mean(this GroupedDataFrame gdf, string aggregateColumnName)
        {
            if (gdf.NativeData != null)
            {
                var col = gdf.Source[aggregateColumnName];
                if (col is IntColumn ic)
                {
                    var native = gdf.NativeData;
                    var meanCol = new DoubleColumn($"Mean_{aggregateColumnName}", native.GroupCount);

                    unsafe
                    {
                        int* pOffsets = native.Offsets.Ptr;
                        int* pIndices = native.Indices.Ptr;
                        int* pKeys = native.Keys.Ptr;

                        fixed (int* pSource = ic.Values.Span)
                        {
                            for (int i = 0; i < native.GroupCount; i++)
                            {
                                int start = pOffsets[i];
                                int end = pOffsets[i + 1];
                                int count = end - start;

                                if (count == 0) { meanCol.Append(0); continue; }

                                double sum = 0;
                                for (int k = start; k < end; k++)
                                {
                                    sum += pSource[pIndices[k]];
                                }
                                meanCol.Append(sum / count);
                            }
                        }
                        // FIX: Use the first column name
                        var keyCol = new IntColumn(gdf.GroupColumnNames[0], native.GroupCount);
                        for (int i = 0; i < native.GroupCount; i++) keyCol.Append(pKeys[i]);
                        return new DataFrame(new IColumn[] { keyCol, meanCol });
                    }
                }
            }

            // --- SLOW PATH ---
            var colManaged = gdf.Source[aggregateColumnName];
            int groupCount = gdf.GroupCount;
            var offsets = gdf.GroupOffsets;
            var indices = gdf.RowIndices;
            bool hasNulls = gdf.NullGroupIndices != null;

            var res = new DoubleColumn($"Mean_{aggregateColumnName}", groupCount + (hasNulls ? 1 : 0));

            if (colManaged is IntColumn icManaged)
            {
                ReadOnlySpan<int> data = icManaged.Values.Span;
                for (int i = 0; i < groupCount; i++)
                {
                    int start = offsets[i];
                    int end = offsets[i + 1];
                    if (start == end) { res.Append(0); continue; }

                    double sum = 0;
                    int count = 0;
                    for (int k = start; k < end; k++)
                    {
                        sum += data[indices[k]];
                        count++;
                    }
                    res.Append(count == 0 ? 0 : sum / count);
                }
                if (hasNulls)
                {
                    double sum = 0; int cnt = 0;
                    foreach (var idx in gdf.NullGroupIndices!) { sum += data[idx]; cnt++; }
                    res.Append(cnt == 0 ? 0 : sum / cnt);
                }
            }
            else if (colManaged is DoubleColumn dcManaged)
            {
                ReadOnlySpan<double> data = dcManaged.Values.Span;
                for (int i = 0; i < groupCount; i++)
                {
                    int start = offsets[i];
                    int end = offsets[i + 1];
                    if (start == end) { res.Append(0); continue; }

                    double sum = 0;
                    int count = 0;
                    for (int k = start; k < end; k++)
                    {
                        sum += data[indices[k]];
                        count++;
                    }
                    res.Append(count == 0 ? 0 : sum / count);
                }
                if (hasNulls)
                {
                    double sum = 0; int cnt = 0;
                    foreach (var idx in gdf.NullGroupIndices!) { sum += data[idx]; cnt++; }
                    res.Append(cnt == 0 ? 0 : sum / cnt);
                }
            }
            else
            {
                throw new NotSupportedException($"Mean not supported for {colManaged.DataType.Name}");
            }

            return CreateResultDataFrame(gdf, $"Mean_{aggregateColumnName}", res);
        }

        // =======================================================================
        // STATE-OF-THE-ART SINGLE-PASS AGGREGATION
        // =======================================================================

        /// <summary>
        /// Performs multiple aggregations on the grouped DataFrame in a single pass.
        /// intelligently reconstructs keys from representative rows for multi-column grouping.
        /// </summary>
        /// <param name="gdf">The grouped DataFrame.</param>
        /// <param name="aggregations">The list of aggregations to perform.</param>
        /// <returns>A new DataFrame containing keys and aggregated results.</returns>
        // =======================================================================
        // STATE-OF-THE-ART SINGLE-PASS AGGREGATION
        // =======================================================================

        public static DataFrame Aggregate(this GroupedDataFrame gdf, params AggregationDef[] aggregations)
        {
            int groupCount = gdf.GroupCount;
            int totalRows = groupCount + (gdf.NullGroupIndices != null ? 1 : 0);

            var resultCols = new List<IColumn>();

            // =========================================================
            // PHASE 1: RECONSTRUCT KEYS
            // =========================================================
            var keysArray = gdf.GetKeys();

            bool isRowIndexKey = gdf is GroupedDataFrame<int> && gdf.GroupColumnNames.Length > 1;

            if (isRowIndexKey)
            {
                // -- Case A: Multi-Column (Lookup values via Row Index) --
                int[] rowIndices = (int[])keysArray;

                foreach (var colName in gdf.GroupColumnNames)
                {
                    var sourceCol = gdf.Source[colName];

                    bool isNullable = sourceCol.IsNullable || gdf.NullGroupIndices != null;

                    var keyCol = ColumnFactory.Create(colName, sourceCol.DataType, totalRows, isNullable: isNullable);

                    // 1. Fill valid groups using representative row
                    for (int i = 0; i < rowIndices.Length; i++)
                    {
                        keyCol.AppendObject(sourceCol.GetValue(rowIndices[i]));
                    }

                    resultCols.Add(keyCol);
                }
            }
            else
            {
                // -- Case B: Single Column (Keys are actual values) --
                string colName = gdf.GroupColumnNames[0];
                Type keyType = keysArray.GetType().GetElementType()!;

                bool isNullable = gdf.NullGroupIndices != null;

                var keyCol = ColumnFactory.Create(colName, keyType, totalRows, isNullable: isNullable);

                for (int i = 0; i < keysArray.Length; i++)
                {
                    keyCol.AppendObject(keysArray.GetValue(i));
                }

                resultCols.Add(keyCol);
            }

            // =========================================================
            // PHASE 2: PREPARE AGGREGATION COLUMNS
            // =========================================================
            var aggTargetCols = new IColumn[aggregations.Length];
            for (int i = 0; i < aggregations.Length; i++)
            {
                var agg = aggregations[i];
                Type targetType = agg.Operation switch
                {
                    AggOp.Count => typeof(int),
                    AggOp.Mean => typeof(double),
                    AggOp.Sum => typeof(double),
                    _ => gdf.Source[agg.SourceColumn].DataType
                };

                aggTargetCols[i] = ColumnFactory.Create(agg.TargetName, targetType, totalRows, isNullable: true);
                resultCols.Add(aggTargetCols[i]);
            }

            // =========================================================
            // PHASE 3: CALCULATE AGGREGATES (Scan CSR)
            // =========================================================
            var offsets = gdf.GroupOffsets;
            var indices = gdf.RowIndices;

            for (int g = 0; g < groupCount; g++)
            {
                int start = offsets[g];
                int end = offsets[g + 1];

                for (int a = 0; a < aggregations.Length; a++)
                {
                    var agg = aggregations[a];
                    object? res = ExecuteSingleAgg(gdf.Source, agg, indices, start, end);
                    aggTargetCols[a].AppendObject(res);
                }
            }

            // =========================================================
            // PHASE 4: HANDLE NULL GROUP
            // =========================================================
            if (gdf.NullGroupIndices is int[] nullIndices)
            {
                // 1. Append Null to all Key Columns
                for (int k = 0; k < gdf.GroupColumnNames.Length; k++)
                {
                    resultCols[k].AppendObject(null);
                }

                // 2. Calculate Aggregates for Null Group
                for (int a = 0; a < aggregations.Length; a++)
                {
                    aggTargetCols[a].AppendObject(
                        ExecuteSingleAgg(gdf.Source, aggregations[a], nullIndices, 0, 0, isNullGroup: true)
                    );
                }
            }

            return new DataFrame(resultCols);
        }

        private static object? ExecuteSingleAgg(DataFrame source, AggregationDef agg, int[] indices, int start, int end, bool isNullGroup = false)
        {
            if (agg.Operation == AggOp.Count) return isNullGroup ? indices.Length : (end - start);

            var col = source[agg.SourceColumn];
            return agg.Operation switch
            {
                AggOp.Sum => col.ComputeSum(indices, start, end),
                AggOp.Mean => col.ComputeMean(indices, start, end),
                AggOp.Min => col.ComputeMin(indices, start, end),
                AggOp.Max => col.ComputeMax(indices, start, end),
                _ => null
            };
        }

        // --- Core Execution Logic (Managed Fallback Helper) ---

        private static DataFrame ExecuteNumericAgg(
            GroupedDataFrame gdf,
            string colName,
            string opName,
            IntSpanNullOp intNullHandler,
            IntSpanCsrOp intCsrHandler,
            DoubleSpanCsrOp dblCsrHandler)
        {
            var col = gdf.Source[colName];
            int groupCount = gdf.GroupCount;
            var offsets = gdf.GroupOffsets;
            var indices = gdf.RowIndices;
            bool hasNulls = gdf.NullGroupIndices != null;

            IColumn resultCol;

            if (col is IntColumn ic)
            {
                var res = new DoubleColumn($"{opName}_{colName}", groupCount + (hasNulls ? 1 : 0));
                ReadOnlySpan<int> data = ic.Values.Span;

                for (int i = 0; i < groupCount; i++)
                {
                    res.Append(intCsrHandler(data, offsets[i], offsets[i + 1], indices));
                }
                if (hasNulls)
                {
                    res.Append(intNullHandler(data, gdf.NullGroupIndices!));
                }
                resultCol = res;
            }
            else if (col is DoubleColumn dc)
            {
                var res = new DoubleColumn($"{opName}_{colName}", groupCount + (hasNulls ? 1 : 0));
                ReadOnlySpan<double> data = dc.Values.Span;

                for (int i = 0; i < groupCount; i++)
                {
                    res.Append(dblCsrHandler(data, offsets[i], offsets[i + 1], indices));
                }
                if (hasNulls)
                {
                    double sum = 0;
                    foreach (var idx in gdf.NullGroupIndices!) sum += data[idx];
                    res.Append(sum);
                }
                resultCol = res;
            }
            else throw new NotSupportedException($"Operation {opName} not supported for {col.DataType.Name}");

            return CreateResultDataFrame(gdf, $"{opName}_{colName}", resultCol);
        }

        private static DataFrame CreateResultDataFrame(GroupedDataFrame gdf, string valColName, IColumn valCol)
        {
            // NEW: Support for Multi-Column Key Reconstruction via Representative Row

            var resultCols = new List<IColumn>();
            int totalCount = gdf.GroupCount + (gdf.NullGroupIndices != null ? 1 : 0);
            var keysArray = gdf.GetKeys();

            // Check if Multi-Column Strategy (Keys are Row Indices)
            bool isRowIndexKey = gdf is GroupedDataFrame<int> && gdf.GroupColumnNames.Length > 1;

            if (isRowIndexKey)
            {
                int[] rowIndices = (int[])keysArray;
                foreach (var colName in gdf.GroupColumnNames)
                {
                    var sourceCol = gdf.Source[colName];
                    var keyCol = ColumnFactory.Create(colName, sourceCol.DataType, totalCount, isNullable: true); // Nullable for safety

                    // Valid Groups
                    for (int i = 0; i < rowIndices.Length; i++)
                    {
                        keyCol.AppendObject(sourceCol.GetValue(rowIndices[i]));
                    }
                    // Null Group
                    if (gdf.NullGroupIndices != null) keyCol.AppendObject(null);

                    resultCols.Add(keyCol);
                }
            }
            else
            {
                // Single Column Strategy (Keys are values)
                // Note: GetKeys returns Array, we assume element type matches column type
                var colName = gdf.GroupColumnNames[0];
                var keyType = keysArray.GetType().GetElementType()!;
                var keyCol = ColumnFactory.Create(colName, keyType, totalCount, isNullable: true);

                foreach (var key in keysArray) keyCol.AppendObject(key);
                if (gdf.NullGroupIndices != null) keyCol.AppendObject(null);

                resultCols.Add(keyCol);
            }

            // Add the calculated Value Column (Count, Sum, etc.)
            resultCols.Add(valCol);

            return new DataFrame(resultCols);
        }
    }
}