namespace LeichtFrame.Core
{
    /// <summary>
    /// Provides extension methods for calculating aggregations (Sum, Min, Max, Mean) on DataFrames.
    /// </summary>
    public static class DataFrameAggregationExtensions
    {
        /// <summary>
        /// Calculates the Sum of a numeric column. Ignores null values.
        /// Returns 0 if column is empty.
        /// </summary>
        /// <param name="df">The DataFrame to operate on.</param>
        /// <param name="columnName">The name of the column to sum.</param>
        /// <returns>The sum as a double.</returns>
        /// <exception cref="NotSupportedException">Thrown if the column type is not numeric.</exception>
        public static double Sum(this DataFrame df, string columnName)
        {
            var col = df[columnName];

            // 1. Double Optimization
            if (col is DoubleColumn doubleCol)
            {
                return doubleCol.Sum();
            }

            // 2. Int Optimization (Direct Span Access)
            if (col is IntColumn intCol)
            {
                var span = intCol.Values.Span;
                long sum = 0;

                // Fast Path for Non-Nullable (No Bit-Check needed)
                if (!intCol.IsNullable)
                {
                    foreach (var val in span) sum += val;
                }
                else
                {
                    // Safe Path for Nullable
                    for (int i = 0; i < intCol.Length; i++)
                    {
                        if (!intCol.IsNull(i)) sum += span[i];
                    }
                }
                return (double)sum;
            }

            throw new NotSupportedException($"Sum operation is not supported for column type '{col.DataType.Name}'.");
        }

        /// <summary>
        /// Calculates the Minimum value of a numeric column. Ignores null values.
        /// Returns 0 (or default) if no values exist.
        /// </summary>
        /// <param name="df">The DataFrame to operate on.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <returns>The minimum value found.</returns>
        public static double Min(this DataFrame df, string columnName)
        {
            var col = df[columnName];

            if (col is DoubleColumn doubleCol) return doubleCol.Min();

            if (col is IntColumn intCol)
            {
                if (intCol.Length == 0) return 0;

                int min = int.MaxValue;
                bool hasValue = false;
                var span = intCol.Values.Span;

                for (int i = 0; i < intCol.Length; i++)
                {
                    if (!intCol.IsNullable || !intCol.IsNull(i))
                    {
                        var val = span[i];
                        if (val < min) min = val;
                        hasValue = true;
                    }
                }
                return hasValue ? min : 0;
            }

            throw new NotSupportedException($"Min operation is not supported for column type '{col.DataType.Name}'.");
        }

        /// <summary>
        /// Calculates the Maximum value of a numeric column. Ignores null values.
        /// </summary>
        /// <param name="df">The DataFrame to operate on.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <returns>The maximum value found.</returns>
        public static double Max(this DataFrame df, string columnName)
        {
            var col = df[columnName];

            if (col is DoubleColumn doubleCol) return doubleCol.Max();

            if (col is IntColumn intCol)
            {
                if (intCol.Length == 0) return 0;

                int max = int.MinValue;
                bool hasValue = false;
                var span = intCol.Values.Span;

                for (int i = 0; i < intCol.Length; i++)
                {
                    if (!intCol.IsNullable || !intCol.IsNull(i))
                    {
                        var val = span[i];
                        if (val > max) max = val;
                        hasValue = true;
                    }
                }
                return hasValue ? max : 0;
            }

            throw new NotSupportedException($"Max operation is not supported for column type '{col.DataType.Name}'.");
        }

        /// <summary>
        /// Calculates the arithmetic Mean (Average) of a numeric column. Ignores null values.
        /// </summary>
        /// <param name="df">The DataFrame to operate on.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <returns>The average value.</returns>
        public static double Mean(this DataFrame df, string columnName)
        {
            // Mean = Sum / Count (of non-nulls)

            var col = df[columnName];
            double sum = df.Sum(columnName);
            int count = 0;

            // We need to count the number of valid (non-null) values
            if (col.IsNullable)
            {
                // Unfortunately, we have to loop here unless we had a "NullCount" property (feature for later?)
                // For now: Simple loop.
                if (col is IntColumn ic)
                {
                    for (int i = 0; i < ic.Length; i++) if (!ic.IsNull(i)) count++;
                }
                else if (col is DoubleColumn dc)
                {
                    for (int i = 0; i < dc.Length; i++) if (!dc.IsNull(i)) count++;
                }
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
    /// </summary>
    public static class GroupAggregationExtensions
    {
        /// <summary>
        /// Aggregates the grouped data by counting rows in each group.
        /// Returns a new DataFrame with columns: [GroupColumn, "Count"].
        /// </summary>
        /// <param name="gdf">The grouped dataframe.</param>
        /// <returns>A new dataframe containing the group keys and their counts.</returns>
        public static DataFrame Count(this GroupedDataFrame gdf)
        {
            // 1. Prepare Columns
            var sourceKeyCol = gdf.Source[gdf.GroupColumnName];

            // Key Column (Same type as source grouping column)
            var keyCol = ColumnFactory.Create(gdf.GroupColumnName, sourceKeyCol.DataType, gdf.GroupMap.Count, sourceKeyCol.IsNullable);

            // Result Column (Always Int for Count)
            var countCol = new IntColumn("Count", gdf.GroupMap.Count);

            // 2. Iterate Groups
            foreach (var kvp in gdf.GroupMap)
            {
                object key = kvp.Key;
                List<int> indices = kvp.Value;

                // Handle Sentinel for Null Key
                object? realKey = DataFrameGroupingExtensions.GetRealValue(key);

                // Helper to append object to specific column type (Reflection/Cast needed here if we don't know T)
                // As our Column API is strongly typed (.Append(int)), but here we have 'object',
                // we need a small helper or casts.
                // For MVP we use a dynamic cast or pattern matching.
                AppendKey(keyCol, realKey);

                // Set Count
                countCol.Append(indices.Count);
            }

            return new DataFrame(new[] { keyCol, countCol });
        }

        /// <summary>
        /// Aggregates the grouped data by summing values in the specified column.
        /// Returns a new DataFrame with columns: [GroupColumn, "Sum_TargetColumn"].
        /// </summary>
        /// <param name="gdf">The grouped dataframe.</param>
        /// <param name="aggregateColumnName">The column to sum up per group.</param>
        /// <returns>A new dataframe containing the group keys and the sums.</returns>
        public static DataFrame Sum(this GroupedDataFrame gdf, string aggregateColumnName)
        {
            var sourceKeyCol = gdf.Source[gdf.GroupColumnName];
            var valueCol = gdf.Source[aggregateColumnName];

            // 1. Prepare Result Structure
            var keyCol = ColumnFactory.Create(gdf.GroupColumnName, sourceKeyCol.DataType, gdf.GroupMap.Count, sourceKeyCol.IsNullable);
            var sumCol = new DoubleColumn($"Sum_{aggregateColumnName}", gdf.GroupMap.Count);

            // 2. Perform Aggregation based on Type
            // We need to distinguish to sum efficiently
            if (valueCol is IntColumn intCol)
            {
                foreach (var kvp in gdf.GroupMap)
                {
                    AppendKey(keyCol, DataFrameGroupingExtensions.GetRealValue(kvp.Key));

                    long groupSum = 0;
                    foreach (var idx in kvp.Value)
                    {
                        // Direct access via Get(i) is fast enough for aggregations
                        if (!intCol.IsNullable || !intCol.IsNull(idx))
                            groupSum += intCol.Get(idx);
                    }
                    sumCol.Append(groupSum);
                }
            }
            else if (valueCol is DoubleColumn dblCol)
            {
                foreach (var kvp in gdf.GroupMap)
                {
                    AppendKey(keyCol, DataFrameGroupingExtensions.GetRealValue(kvp.Key));

                    double groupSum = 0;
                    foreach (var idx in kvp.Value)
                    {
                        if (!dblCol.IsNullable || !dblCol.IsNull(idx))
                            groupSum += dblCol.Get(idx);
                    }
                    sumCol.Append(groupSum);
                }
            }
            else
            {
                throw new NotSupportedException($"Sum not supported for column '{aggregateColumnName}' of type {valueCol.DataType.Name}");
            }

            return new DataFrame(new IColumn[] { keyCol, sumCol });
        }

        // --- Helper to append untyped object to typed column ---
        private static void AppendKey(IColumn col, object? value)
        {
            if (value == null)
            {
                // Reflection to find "Append(T?)" is hard, but we can assume concrete types for MVP
                if (col is IntColumn ic) ic.Append(null);
                else if (col is DoubleColumn dc) dc.Append(null);
                else if (col is StringColumn sc) sc.Append(null);
                else if (col is BoolColumn bc) bc.Append(null);
                else if (col is DateTimeColumn dtc) dtc.Append(null);
                return;
            }

            if (col is IntColumn i) i.Append((int)value);
            else if (col is StringColumn s) s.Append((string)value);
            else if (col is DoubleColumn d) d.Append((double)value);
            else if (col is BoolColumn b) b.Append((bool)value);
            else if (col is DateTimeColumn dt) dt.Append((DateTime)value);
            else throw new NotSupportedException($"Unknown column type for key: {col.GetType().Name}");
        }
    }
}