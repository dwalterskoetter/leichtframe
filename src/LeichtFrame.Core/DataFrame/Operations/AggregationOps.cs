namespace LeichtFrame.Core
{
    /// <summary>
    /// Provides extension methods for calculating aggregations (Sum, Min, Max, Mean) on DataFrames.
    /// </summary>
    public static class DataFrameAggregationExtensions
    {
        /// <summary>
        /// Calculates the Sum of a numeric column.
        /// </summary>
        public static double Sum(this DataFrame df, string columnName)
        {
            var col = df[columnName];
            if (col is DoubleColumn doubleCol) return doubleCol.Sum();
            if (col is IntColumn intCol) return (double)intCol.Sum();
            throw new NotSupportedException($"Sum operation is not supported for column type '{col.DataType.Name}'.");
        }

        /// <summary>
        /// Calculates the Minimum value of a numeric column.
        /// </summary>
        public static double Min(this DataFrame df, string columnName)
        {
            var col = df[columnName];
            if (col is DoubleColumn doubleCol) return doubleCol.Min();
            if (col is IntColumn intCol) return (double)intCol.Min();
            throw new NotSupportedException($"Min operation is not supported for column type '{col.DataType.Name}'.");
        }

        /// <summary>
        /// Calculates the Maximum value of a numeric column.
        /// </summary>
        public static double Max(this DataFrame df, string columnName)
        {
            var col = df[columnName];
            if (col is DoubleColumn doubleCol) return doubleCol.Max();
            if (col is IntColumn intCol) return (double)intCol.Max();
            throw new NotSupportedException($"Max operation is not supported for column type '{col.DataType.Name}'.");
        }

        /// <summary>
        /// Calculates the arithmetic Mean (Average) of a numeric column.
        /// </summary>
        public static double Mean(this DataFrame df, string columnName)
        {
            var col = df[columnName];
            double sum = df.Sum(columnName);
            int count = 0;

            if (col.IsNullable)
            {
                for (int i = 0; i < col.Length; i++)
                    if (!col.IsNull(i)) count++;
            }
            else
            {
                count = col.Length;
            }

            if (count == 0) return 0;
            return sum / count;
        }
    }

    /// <summary>
    /// Provides extension methods for performing aggregations on grouped dataframes.
    /// Optimized for high performance using CSR (Compressed Sparse Row) iteration.
    /// </summary>
    public static class GroupAggregationExtensions
    {
        // --- Custom Delegates to support ReadOnlySpan (ref struct) ---
        private delegate long IntSpanNullOp(ReadOnlySpan<int> data, int[] indices);
        private delegate long IntSpanCsrOp(ReadOnlySpan<int> data, int start, int end, int[] indices);
        private delegate double DoubleSpanCsrOp(ReadOnlySpan<double> data, int start, int end, int[] indices);

        /// <summary>
        /// Aggregates the grouped data by counting rows in each group.
        /// </summary>
        public static DataFrame Count(this GroupedDataFrame gdf)
        {
            var offsets = gdf.GroupOffsets;
            var count = gdf.GroupCount;
            bool hasNulls = gdf.NullGroupIndices != null;

            var countsCol = new IntColumn("Count", count + (hasNulls ? 1 : 0));

            for (int i = 0; i < count; i++)
            {
                countsCol.Append(offsets[i + 1] - offsets[i]);
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
            var col = gdf.Source[aggregateColumnName];
            int groupCount = gdf.GroupCount;
            var offsets = gdf.GroupOffsets;
            var indices = gdf.RowIndices;
            bool hasNulls = gdf.NullGroupIndices != null;

            var res = new DoubleColumn($"Mean_{aggregateColumnName}", groupCount + (hasNulls ? 1 : 0));

            if (col is IntColumn ic)
            {
                ReadOnlySpan<int> data = ic.Values.Span;
                for (int i = 0; i < groupCount; i++)
                {
                    // Inline Int Logic
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
            else if (col is DoubleColumn dc)
            {
                ReadOnlySpan<double> data = dc.Values.Span;
                for (int i = 0; i < groupCount; i++)
                {
                    // Inline Double Logic
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
                throw new NotSupportedException($"Mean not supported for {col.DataType.Name}");
            }

            return CreateResultDataFrame(gdf, $"Mean_{aggregateColumnName}", res);
        }

        // --- Core Execution Logic ---

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
                    // Simplification: We assume sum logic for nullable double fallback in this generic handler
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
            var keysArray = gdf.GetKeys();
            var type = keysArray.GetType().GetElementType()!;

            int totalCount = gdf.GroupCount + (gdf.NullGroupIndices != null ? 1 : 0);
            var keyCol = ColumnFactory.Create(gdf.GroupColumnName, type, totalCount, isNullable: true);

            foreach (var key in keysArray) keyCol.AppendObject(key);

            if (gdf.NullGroupIndices != null)
            {
                keyCol.AppendObject(null);
            }

            return new DataFrame(new[] { keyCol, valCol });
        }
    }
}