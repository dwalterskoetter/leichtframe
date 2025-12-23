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
            if (gdf.NativeData != null && !gdf.KeysAreRowIndices)
            {
                var native = gdf.NativeData;
                bool hasNulls = gdf.NullGroupIndices != null && gdf.NullGroupIndices.Length > 0;
                int totalRows = native.GroupCount + (hasNulls ? 1 : 0);

                var nativeCounts = new IntColumn("Count", totalRows);
                var keyCol = new IntColumn(gdf.GroupColumnNames[0], totalRows, isNullable: hasNulls);

                unsafe
                {
                    int* offsets = native.Offsets.Ptr;
                    int* keys = native.Keys.Ptr;

                    for (int i = 0; i < native.GroupCount; i++)
                    {
                        nativeCounts.Append(offsets[i + 1] - offsets[i]);
                        keyCol.Append(keys[i]);
                    }
                }

                if (hasNulls)
                {
                    nativeCounts.Append(gdf.NullGroupIndices!.Length);
                    keyCol.Append(null);
                }

                return new DataFrame(new IColumn[] { keyCol, nativeCounts });
            }

            // --- SLOW PATH: Managed Arrays (Fallback) ---
            var offsetsManaged = gdf.GroupOffsets;
            var count = gdf.GroupCount;
            bool hasNullsSlow = gdf.NullGroupIndices != null && gdf.NullGroupIndices.Length > 0;

            var countsCol = new IntColumn("Count", count + (hasNullsSlow ? 1 : 0));

            for (int i = 0; i < count; i++)
            {
                countsCol.Append(offsetsManaged[i + 1] - offsetsManaged[i]);
            }

            if (hasNullsSlow)
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
            if (gdf.NativeData != null && !gdf.KeysAreRowIndices)
            {
                var col = gdf.Source[aggregateColumnName];

                if (col is IntColumn ic)
                {
                    var native = gdf.NativeData;
                    bool hasNulls = gdf.NullGroupIndices != null && gdf.NullGroupIndices.Length > 0;
                    int totalRows = native.GroupCount + (hasNulls ? 1 : 0);

                    var sumCol = new DoubleColumn($"Sum_{aggregateColumnName}", totalRows);
                    var keyCol = new IntColumn(gdf.GroupColumnNames[0], totalRows, isNullable: hasNulls);

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
                                keyCol.Append(pKeys[i]);
                            }
                        }
                    }

                    if (hasNulls)
                    {
                        long sumNull = 0;
                        var span = ic.Values.Span;
                        foreach (var idx in gdf.NullGroupIndices!) sumNull += span[idx];
                        sumCol.Append(sumNull);
                        keyCol.Append(null);
                    }

                    return new DataFrame(new IColumn[] { keyCol, sumCol });
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
            if (gdf.NativeData != null && !gdf.KeysAreRowIndices)
            {
                var col = gdf.Source[aggregateColumnName];
                if (col is IntColumn ic)
                {
                    var native = gdf.NativeData;
                    bool hasNulls = gdf.NullGroupIndices != null && gdf.NullGroupIndices.Length > 0;
                    int totalRows = native.GroupCount + (hasNulls ? 1 : 0);

                    var minCol = new IntColumn($"Min_{aggregateColumnName}", totalRows);
                    var keyCol = new IntColumn(gdf.GroupColumnNames[0], totalRows, isNullable: hasNulls);

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
                                if (start == end) { minCol.Append(0); keyCol.Append(pKeys[i]); continue; }

                                int min = int.MaxValue;
                                for (int k = start; k < end; k++)
                                {
                                    int val = pSource[pIndices[k]];
                                    if (val < min) min = val;
                                }
                                minCol.Append(min);
                                keyCol.Append(pKeys[i]);
                            }
                        }
                    }

                    if (hasNulls)
                    {
                        int min = int.MaxValue;
                        var span = ic.Values.Span;
                        foreach (var idx in gdf.NullGroupIndices!) if (span[idx] < min) min = span[idx];
                        minCol.Append(min);
                        keyCol.Append(null);
                    }

                    return new DataFrame(new IColumn[] { keyCol, minCol });
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
            if (gdf.NativeData != null && !gdf.KeysAreRowIndices)
            {
                var col = gdf.Source[aggregateColumnName];
                if (col is IntColumn ic)
                {
                    var native = gdf.NativeData;
                    bool hasNulls = gdf.NullGroupIndices != null && gdf.NullGroupIndices.Length > 0;
                    int totalRows = native.GroupCount + (hasNulls ? 1 : 0);

                    var maxCol = new IntColumn($"Max_{aggregateColumnName}", totalRows);
                    var keyCol = new IntColumn(gdf.GroupColumnNames[0], totalRows, isNullable: hasNulls);

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
                                if (start == end) { maxCol.Append(0); keyCol.Append(pKeys[i]); continue; }

                                int max = int.MinValue;
                                for (int k = start; k < end; k++)
                                {
                                    int val = pSource[pIndices[k]];
                                    if (val > max) max = val;
                                }
                                maxCol.Append(max);
                                keyCol.Append(pKeys[i]);
                            }
                        }
                    }

                    if (hasNulls)
                    {
                        int max = int.MinValue;
                        var span = ic.Values.Span;
                        foreach (var idx in gdf.NullGroupIndices!) if (span[idx] > max) max = span[idx];
                        maxCol.Append(max);
                        keyCol.Append(null);
                    }

                    return new DataFrame(new IColumn[] { keyCol, maxCol });
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
            if (gdf.NativeData != null && !gdf.KeysAreRowIndices)
            {
                var col = gdf.Source[aggregateColumnName];
                if (col is IntColumn ic)
                {
                    var native = gdf.NativeData;
                    bool hasNulls = gdf.NullGroupIndices != null && gdf.NullGroupIndices.Length > 0;
                    int totalRows = native.GroupCount + (hasNulls ? 1 : 0);

                    var meanCol = new DoubleColumn($"Mean_{aggregateColumnName}", totalRows);
                    var keyCol = new IntColumn(gdf.GroupColumnNames[0], totalRows, isNullable: hasNulls);

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

                                if (count == 0) { meanCol.Append(0); keyCol.Append(pKeys[i]); continue; }

                                double sum = 0;
                                for (int k = start; k < end; k++)
                                {
                                    sum += pSource[pIndices[k]];
                                }
                                meanCol.Append(sum / count);
                                keyCol.Append(pKeys[i]);
                            }
                        }
                    }

                    if (hasNulls)
                    {
                        double sum = 0;
                        int count = gdf.NullGroupIndices!.Length;
                        var span = ic.Values.Span;
                        foreach (var idx in gdf.NullGroupIndices!) sum += span[idx];
                        meanCol.Append(count == 0 ? 0 : sum / count);
                        keyCol.Append(null);
                    }

                    return new DataFrame(new IColumn[] { keyCol, meanCol });
                }
            }

            // --- SLOW PATH ---
            var colManaged = gdf.Source[aggregateColumnName];
            int groupCount = gdf.GroupCount;
            var offsets = gdf.GroupOffsets;
            var indices = gdf.RowIndices;
            bool hasNullsSlow = gdf.NullGroupIndices != null;

            var res = new DoubleColumn($"Mean_{aggregateColumnName}", groupCount + (hasNullsSlow ? 1 : 0));

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
                if (hasNullsSlow)
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
                if (hasNullsSlow)
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
        // AGGREGATE (GENERAL)
        // =======================================================================

        /// <summary>
        /// Aggregation DataFrame
        /// </summary>
        /// <param name="gdf"></param>
        /// <param name="aggregations"></param>
        /// <returns></returns>
        public static DataFrame Aggregate(this GroupedDataFrame gdf, params AggregationDef[] aggregations)
        {
            int groupCount = gdf.GroupCount;
            int totalRows = groupCount + (gdf.NullGroupIndices != null ? 1 : 0);

            var resultCols = new List<IColumn>();

            // =========================================================
            // PHASE 1: RECONSTRUCT KEYS
            // =========================================================
            var keysArray = gdf.GetKeys();

            bool isRowIndexKeyMultiCol = (gdf.KeysAreRowIndices && gdf.GroupColumnNames.Length > 1)
                          || (gdf is GroupedDataFrame<int> && gdf.GroupColumnNames.Length > 1 && !gdf.KeysAreRowIndices);

            if (isRowIndexKeyMultiCol)
            {
                int[] rowIndices = (int[])keysArray;

                foreach (var colName in gdf.GroupColumnNames)
                {
                    var sourceCol = gdf.Source[colName];
                    bool isNullable = sourceCol.IsNullable || gdf.NullGroupIndices != null;
                    var keyCol = ColumnFactory.Create(colName, sourceCol.DataType, totalRows, isNullable: isNullable);

                    for (int i = 0; i < rowIndices.Length; i++)
                    {
                        keyCol.AppendObject(sourceCol.GetValue(rowIndices[i]));
                    }
                    resultCols.Add(keyCol);
                }
            }
            else
            {
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
            // PHASE 3: CALCULATE AGGREGATES
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
                // Append Null to Keys
                for (int k = 0; k < gdf.GroupColumnNames.Length; k++)
                {
                    resultCols[k].AppendObject(null);
                }

                // Calc Aggs for Nulls
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

        // --- Helper: Create Result DataFrame ---
        private static DataFrame CreateResultDataFrame(GroupedDataFrame gdf, string valColName, IColumn valCol)
        {
            var resultCols = new List<IColumn>();
            int totalCount = gdf.GroupCount + (gdf.NullGroupIndices != null ? 1 : 0);
            var keysArray = gdf.GetKeys();
            bool isRowIndexKeyMultiCol = (gdf.KeysAreRowIndices && gdf.GroupColumnNames.Length > 1)
                          || (gdf is GroupedDataFrame<int> && gdf.GroupColumnNames.Length > 1 && !gdf.KeysAreRowIndices);

            if (isRowIndexKeyMultiCol)
            {
                int[] rowIndices = (int[])keysArray;
                foreach (var colName in gdf.GroupColumnNames)
                {
                    var sourceCol = gdf.Source[colName];
                    var keyCol = ColumnFactory.Create(colName, sourceCol.DataType, totalCount, isNullable: true);

                    for (int i = 0; i < rowIndices.Length; i++)
                    {
                        keyCol.AppendObject(sourceCol.GetValue(rowIndices[i]));
                    }
                    if (gdf.NullGroupIndices != null) keyCol.AppendObject(null);

                    resultCols.Add(keyCol);
                }
            }
            else
            {
                var colName = gdf.GroupColumnNames[0];
                var keyType = keysArray.GetType().GetElementType()!;
                var keyCol = ColumnFactory.Create(colName, keyType, totalCount, isNullable: true);

                foreach (var key in keysArray) keyCol.AppendObject(key);
                if (gdf.NullGroupIndices != null) keyCol.AppendObject(null);

                resultCols.Add(keyCol);
            }

            resultCols.Add(valCol);
            return new DataFrame(resultCols);
        }

        // --- Helper: Generic Numeric Agg Execution ---
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
                // Falls Operation double returniert (Mean), wird das hier berücksichtigt?
                // Derzeit wird DoubleColumn für alle Ergebnisse genutzt in diesem Helper.
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
    }
}