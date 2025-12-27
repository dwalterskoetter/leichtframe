using LeichtFrame.Core.Operations.Aggregate;

namespace LeichtFrame.Core.Engine.Kernels.Aggregate.Strategies
{
    /// <summary>
    /// Implements aggregation logic using safe managed code and arrays.
    /// Handles Mean, Double types, and Multi-Column grouping fallback.
    /// </summary>
    internal class ManagedAggregateStrategy : IAggregateStrategy
    {
        // Delegates
        private delegate long IntSpanNullOp(ReadOnlySpan<int> data, int[] indices);
        private delegate long IntSpanCsrOp(ReadOnlySpan<int> data, int start, int end, int[] indices);
        private delegate double DoubleSpanCsrOp(ReadOnlySpan<double> data, int start, int end, int[] indices);

        public DataFrame Count(GroupedDataFrame gdf)
        {
            var offsetsManaged = gdf.GroupOffsets;
            var count = gdf.GroupCount;
            bool hasNullsSlow = gdf.NullGroupIndices != null && gdf.NullGroupIndices.Length > 0;

            var countsCol = new IntColumn("Count", count + (hasNullsSlow ? 1 : 0));
            for (int i = 0; i < count; i++)
                countsCol.Append(offsetsManaged[i + 1] - offsetsManaged[i]);

            if (hasNullsSlow) countsCol.Append(gdf.NullGroupIndices!.Length);

            return CreateResultDataFrame(gdf, "Count", countsCol);
        }

        public DataFrame Sum(GroupedDataFrame gdf, string colName)
        {
            return ExecuteNumericAgg(gdf, colName, "Sum", typeof(double),
                (span, indices) => { long s = 0; foreach (var i in indices) s += span[i]; return s; },
                (span, s, e, idx) => { long sum = 0; for (int k = s; k < e; k++) sum += span[idx[k]]; return sum; },
                (span, s, e, idx) => { double sum = 0; for (int k = s; k < e; k++) sum += span[idx[k]]; return sum; }
            );
        }

        public DataFrame Min(GroupedDataFrame gdf, string colName)
        {
            return ExecuteNumericAgg(gdf, colName, "Min", typeof(double),
                (span, indices) => { int m = int.MaxValue; foreach (var i in indices) if (span[i] < m) m = span[i]; return m; },
                (span, s, e, idx) => { int m = int.MaxValue; for (int k = s; k < e; k++) if (span[idx[k]] < m) m = span[idx[k]]; return m == int.MaxValue ? 0 : m; },
                (span, s, e, idx) => { double m = double.MaxValue; for (int k = s; k < e; k++) if (span[idx[k]] < m) m = span[idx[k]]; return m == double.MaxValue ? 0 : m; }
            );
        }

        public DataFrame Max(GroupedDataFrame gdf, string colName)
        {
            return ExecuteNumericAgg(gdf, colName, "Max", typeof(double),
                (span, indices) => { int m = int.MinValue; foreach (var i in indices) if (span[i] > m) m = span[i]; return m; },
                (span, s, e, idx) => { int m = int.MinValue; for (int k = s; k < e; k++) if (span[idx[k]] > m) m = span[idx[k]]; return m == int.MinValue ? 0 : m; },
                (span, s, e, idx) => { double m = double.MinValue; for (int k = s; k < e; k++) if (span[idx[k]] > m) m = span[idx[k]]; return m == double.MinValue ? 0 : m; }
            );
        }

        public DataFrame Mean(GroupedDataFrame gdf, string colName)
        {
            var col = gdf.Source[colName];
            int groupCount = gdf.GroupCount;
            var offsets = gdf.GroupOffsets;
            var indices = gdf.RowIndices;
            bool hasNulls = gdf.NullGroupIndices != null && gdf.NullGroupIndices.Length > 0;

            var res = new DoubleColumn($"Mean_{colName}", groupCount + (hasNulls ? 1 : 0));

            if (col is IntColumn ic)
            {
                ReadOnlySpan<int> data = ic.Values.Span;
                for (int i = 0; i < groupCount; i++)
                {
                    int start = offsets[i];
                    int end = offsets[i + 1];
                    int count = end - start;
                    if (count == 0) { res.Append(0); continue; }

                    long sum = 0;
                    for (int k = start; k < end; k++) sum += data[indices[k]];
                    res.Append((double)sum / count);
                }

                if (hasNulls)
                {
                    long sum = 0;
                    int count = gdf.NullGroupIndices!.Length;
                    foreach (var idx in gdf.NullGroupIndices) sum += data[idx];
                    res.Append(count == 0 ? 0 : (double)sum / count);
                }
            }
            else if (col is DoubleColumn dc)
            {
                ReadOnlySpan<double> data = dc.Values.Span;
                for (int i = 0; i < groupCount; i++)
                {
                    int start = offsets[i];
                    int end = offsets[i + 1];
                    int count = end - start;
                    if (count == 0) { res.Append(0); continue; }

                    double sum = 0;
                    for (int k = start; k < end; k++) sum += data[indices[k]];
                    res.Append(sum / count);
                }

                if (hasNulls)
                {
                    double sum = 0;
                    int count = gdf.NullGroupIndices!.Length;
                    foreach (var idx in gdf.NullGroupIndices) sum += data[idx];
                    res.Append(count == 0 ? 0 : sum / count);
                }
            }
            else
            {
                throw new NotSupportedException($"Mean not supported for column type {col.DataType.Name}");
            }

            return CreateResultDataFrame(gdf, $"Mean_{colName}", res);
        }

        public DataFrame Aggregate(GroupedDataFrame gdf, AggregationDef[] aggregations)
        {
            int groupCount = gdf.GroupCount;
            bool hasNulls = gdf.NullGroupIndices != null && gdf.NullGroupIndices.Length > 0;
            int totalRows = groupCount + (hasNulls ? 1 : 0);
            var resultCols = new List<IColumn>();

            // 1. Reconstruct Keys (and add them to resultCols)
            var keysArray = gdf.GetKeys();
            bool isRowIndexKeyMultiCol = (gdf.KeysAreRowIndices && gdf.GroupColumnNames.Length > 1)
                                         || (gdf is GroupedDataFrame<int> && gdf.GroupColumnNames.Length > 1 && !gdf.KeysAreRowIndices);

            if (isRowIndexKeyMultiCol)
            {
                int[] rowIndices = (int[])keysArray;
                foreach (var colName in gdf.GroupColumnNames)
                {
                    var sourceCol = gdf.Source[colName];
                    var keyCol = ColumnFactory.Create(colName, sourceCol.DataType, totalRows, isNullable: true);
                    for (int i = 0; i < rowIndices.Length; i++) keyCol.AppendObject(sourceCol.GetValue(rowIndices[i]));
                    if (hasNulls) keyCol.AppendObject(null);
                    resultCols.Add(keyCol);
                }
            }
            else
            {
                var colName = gdf.GroupColumnNames[0];
                var keyType = keysArray.GetType().GetElementType()!;
                var keyCol = ColumnFactory.Create(colName, keyType, totalRows, isNullable: true);
                for (int i = 0; i < keysArray.Length; i++) keyCol.AppendObject(keysArray.GetValue(i));
                if (hasNulls) keyCol.AppendObject(null);
                resultCols.Add(keyCol);
            }

            // 2. Create Aggregation Target Columns
            for (int i = 0; i < aggregations.Length; i++)
            {
                var agg = aggregations[i];
                Type targetType = typeof(double);

                if (agg.Operation == AggOp.Count)
                    targetType = typeof(int);
                else if ((agg.Operation == AggOp.Min || agg.Operation == AggOp.Max) && gdf.Source[agg.SourceColumn].DataType == typeof(int))
                    targetType = typeof(int);

                var aggCol = ColumnFactory.Create(agg.TargetName, targetType, totalRows, isNullable: true);
                resultCols.Add(aggCol);
            }

            // 3. Compute Aggregates
            var offsets = gdf.GroupOffsets;
            var indices = gdf.RowIndices;
            int aggColsCount = aggregations.Length;
            int keyColsCount = resultCols.Count - aggColsCount;

            // Iterate Groups
            for (int g = 0; g < groupCount; g++)
            {
                int start = offsets[g];
                int end = offsets[g + 1];
                for (int a = 0; a < aggColsCount; a++)
                {
                    object? res = ExecuteSingleAgg(gdf.Source, aggregations[a], indices, start, end);
                    resultCols[keyColsCount + a].AppendObject(res);
                }
            }

            // 4. Compute Null Group Aggregates
            if (hasNulls)
            {
                for (int a = 0; a < aggColsCount; a++)
                {
                    object? res = ExecuteSingleAgg(gdf.Source, aggregations[a], gdf.NullGroupIndices!, 0, 0, true);
                    resultCols[keyColsCount + a].AppendObject(res);
                }
            }

            return new DataFrame(resultCols);
        }

        // --- Helpers ---

        private static DataFrame CreateResultDataFrame(GroupedDataFrame gdf, string valColName, IColumn valCol)
        {
            var resultCols = new List<IColumn>();
            bool hasNulls = gdf.NullGroupIndices != null && gdf.NullGroupIndices.Length > 0;
            int totalCount = gdf.GroupCount + (hasNulls ? 1 : 0);

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
                    for (int i = 0; i < rowIndices.Length; i++) keyCol.AppendObject(sourceCol.GetValue(rowIndices[i]));
                    if (hasNulls) keyCol.AppendObject(null);
                    resultCols.Add(keyCol);
                }
            }
            else
            {
                var colName = gdf.GroupColumnNames[0];
                var keyType = keysArray.GetType().GetElementType()!;
                var keyCol = ColumnFactory.Create(colName, keyType, totalCount, isNullable: true);
                for (int i = 0; i < keysArray.Length; i++) keyCol.AppendObject(keysArray.GetValue(i));
                if (hasNulls) keyCol.AppendObject(null);
                resultCols.Add(keyCol);
            }
            resultCols.Add(valCol);
            return new DataFrame(resultCols);
        }

        private static DataFrame ExecuteNumericAgg(
            GroupedDataFrame gdf,
            string colName,
            string opName,
            Type resultType,
            IntSpanNullOp intNullHandler,
            IntSpanCsrOp intCsrHandler,
            DoubleSpanCsrOp dblCsrHandler)
        {
            var col = gdf.Source[colName];
            int groupCount = gdf.GroupCount;
            var offsets = gdf.GroupOffsets;
            var indices = gdf.RowIndices;
            bool hasNulls = gdf.NullGroupIndices != null && gdf.NullGroupIndices.Length > 0;

            IColumn resultCol;

            if (col is IntColumn ic)
            {
                if (resultType == typeof(int))
                {
                    var res = new IntColumn($"{opName}_{colName}", groupCount + (hasNulls ? 1 : 0));
                    ReadOnlySpan<int> data = ic.Values.Span;
                    for (int i = 0; i < groupCount; i++) res.Append((int)intCsrHandler(data, offsets[i], offsets[i + 1], indices));
                    if (hasNulls) res.Append((int)intNullHandler(data, gdf.NullGroupIndices!));
                    resultCol = res;
                }
                else
                {
                    var res = new DoubleColumn($"{opName}_{colName}", groupCount + (hasNulls ? 1 : 0));
                    ReadOnlySpan<int> data = ic.Values.Span;
                    for (int i = 0; i < groupCount; i++) res.Append(intCsrHandler(data, offsets[i], offsets[i + 1], indices));
                    if (hasNulls) res.Append(intNullHandler(data, gdf.NullGroupIndices!));
                    resultCol = res;
                }
            }
            else if (col is DoubleColumn dc)
            {
                var res = new DoubleColumn($"{opName}_{colName}", groupCount + (hasNulls ? 1 : 0));
                ReadOnlySpan<double> data = dc.Values.Span;
                for (int i = 0; i < groupCount; i++) res.Append(dblCsrHandler(data, offsets[i], offsets[i + 1], indices));
                if (hasNulls) { double sum = 0; foreach (var idx in gdf.NullGroupIndices!) sum += data[idx]; res.Append(sum); }
                resultCol = res;
            }
            else throw new NotSupportedException($"Operation {opName} not supported for {col.DataType.Name}");

            return CreateResultDataFrame(gdf, $"{opName}_{colName}", resultCol);
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
    }
}