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
        /// <param name="df">The source DataFrame.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <returns>The sum of all values.</returns>
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
        /// <param name="df">The source DataFrame.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <returns>The minimum value.</returns>
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
        /// <param name="df">The source DataFrame.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <returns>The maximum value.</returns>
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
        /// <param name="df">The source DataFrame.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <returns>The mean value.</returns>
        public static double Mean(this DataFrame df, string columnName)
        {
            var col = df[columnName];
            double sum = df.Sum(columnName);
            int count = 0;

            if (col.IsNullable)
            {
                // Simple loop to count non-nulls.
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
    /// Optimized for high performance (Zero-Allocation per group).
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
            // For Count, the value column doesn't matter, we can pass the key column as a placeholder.
            var keyCol = gdf.Source[gdf.GroupColumnName];
            return ExecuteAggregation(gdf, keyCol, "Count", (indices, _) => indices.Count);
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
            return ExecuteNumericAggregation(gdf, aggregateColumnName, "Sum",
                (indices, col) =>
                {
                    long sum = 0;
                    foreach (var i in indices)
                    {
                        if (!col.IsNull(i)) sum += col.Get(i);
                    }
                    return sum;
                },
                (indices, col) =>
                {
                    double sum = 0;
                    foreach (var i in indices)
                    {
                        if (!col.IsNull(i)) sum += col.Get(i);
                    }
                    return sum;
                }
            );
        }

        /// <summary>
        /// Aggregates the grouped data by finding the minimum value in the specified column.
        /// Returns a new DataFrame with columns: [GroupColumn, "Min_TargetColumn"].
        /// </summary>
        /// <param name="gdf">The grouped dataframe.</param>
        /// <param name="aggregateColumnName">The target column.</param>
        /// <returns>A new dataframe containing the group keys and the minimums.</returns>
        public static DataFrame Min(this GroupedDataFrame gdf, string aggregateColumnName)
        {
            return ExecuteNumericAggregation(gdf, aggregateColumnName, "Min",
                (indices, col) =>
                {
                    if (indices.Count == 0) return 0;
                    int min = int.MaxValue;
                    bool hasVal = false;
                    foreach (var i in indices)
                    {
                        if (!col.IsNull(i))
                        {
                            int v = col.Get(i);
                            if (v < min) min = v;
                            hasVal = true;
                        }
                    }
                    return hasVal ? min : 0;
                },
                (indices, col) =>
                {
                    if (indices.Count == 0) return 0;
                    double min = double.MaxValue;
                    bool hasVal = false;
                    foreach (var i in indices)
                    {
                        if (!col.IsNull(i))
                        {
                            double v = col.Get(i);
                            if (v < min) min = v;
                            hasVal = true;
                        }
                    }
                    return hasVal ? min : 0;
                }
            );
        }

        /// <summary>
        /// Aggregates the grouped data by finding the maximum value in the specified column.
        /// Returns a new DataFrame with columns: [GroupColumn, "Max_TargetColumn"].
        /// </summary>
        /// <param name="gdf">The grouped dataframe.</param>
        /// <param name="aggregateColumnName">The target column.</param>
        /// <returns>A new dataframe containing the group keys and the maximums.</returns>
        public static DataFrame Max(this GroupedDataFrame gdf, string aggregateColumnName)
        {
            return ExecuteNumericAggregation(gdf, aggregateColumnName, "Max",
                (indices, col) =>
                {
                    if (indices.Count == 0) return 0;
                    int max = int.MinValue;
                    bool hasVal = false;
                    foreach (var i in indices)
                    {
                        if (!col.IsNull(i))
                        {
                            int v = col.Get(i);
                            if (v > max) max = v;
                            hasVal = true;
                        }
                    }
                    return hasVal ? max : 0;
                },
                (indices, col) =>
                {
                    if (indices.Count == 0) return 0;
                    double max = double.MinValue;
                    bool hasVal = false;
                    foreach (var i in indices)
                    {
                        if (!col.IsNull(i))
                        {
                            double v = col.Get(i);
                            if (v > max) max = v;
                            hasVal = true;
                        }
                    }
                    return hasVal ? max : 0;
                }
            );
        }

        /// <summary>
        /// Aggregates the grouped data by calculating the mean value in the specified column.
        /// Returns a new DataFrame with columns: [GroupColumn, "Mean_TargetColumn"].
        /// </summary>
        /// <param name="gdf">The grouped dataframe.</param>
        /// <param name="aggregateColumnName">The target column.</param>
        /// <returns>A new dataframe containing the group keys and the means.</returns>
        public static DataFrame Mean(this GroupedDataFrame gdf, string aggregateColumnName)
        {
            return ExecuteNumericAggregation(gdf, aggregateColumnName, "Mean",
                (indices, col) =>
                {
                    long sum = 0;
                    int count = 0;
                    foreach (var i in indices)
                    {
                        if (!col.IsNull(i))
                        {
                            sum += col.Get(i);
                            count++;
                        }
                    }
                    return count == 0 ? 0 : (double)sum / count;
                },
                (indices, col) =>
                {
                    double sum = 0;
                    int count = 0;
                    foreach (var i in indices)
                    {
                        if (!col.IsNull(i))
                        {
                            sum += col.Get(i);
                            count++;
                        }
                    }
                    return count == 0 ? 0 : sum / count;
                }
            );
        }

        // --- Private Helpers ---

        private delegate double TypedAggregator<TColumn>(List<int> indices, TColumn col);

        private static DataFrame ExecuteNumericAggregation(
            GroupedDataFrame gdf,
            string inputColName,
            string operationPrefix,
            TypedAggregator<IntColumn> intOp,
            TypedAggregator<DoubleColumn> doubleOp)
        {
            // Resolve the actual value column (e.g. "Salary")
            var valueCol = gdf.Source[inputColName];

            Func<List<int>, IColumn, double> calculator;

            if (valueCol is IntColumn)
            {
                calculator = (indices, col) => intOp(indices, (IntColumn)col);
            }
            else if (valueCol is DoubleColumn)
            {
                calculator = (indices, col) => doubleOp(indices, (DoubleColumn)col);
            }
            else
            {
                throw new NotSupportedException($"Operation {operationPrefix} not supported for type {valueCol.DataType.Name}");
            }

            // Important: Pass 'valueCol' down to ExecuteAggregation
            return ExecuteAggregation(gdf, valueCol, $"{operationPrefix}_{inputColName}", calculator);
        }

        private static DataFrame ExecuteAggregation(
            GroupedDataFrame gdf,
            IColumn valueCol,
            string resultColName,
            Func<List<int>, IColumn, double> aggFunc)
        {
            var sourceKeyCol = gdf.Source[gdf.GroupColumnName];
            var keyCol = ColumnFactory.Create(gdf.GroupColumnName, sourceKeyCol.DataType, gdf.GroupMap.Count, sourceKeyCol.IsNullable);

            IColumn resultCol;
            if (resultColName == "Count")
                resultCol = new IntColumn(resultColName, gdf.GroupMap.Count);
            else
                resultCol = new DoubleColumn(resultColName, gdf.GroupMap.Count);

            foreach (var kvp in gdf.GroupMap)
            {
                object? realKey = DataFrameGroupingExtensions.GetRealValue(kvp.Key);
                keyCol.AppendObject(realKey);

                // Pass the correct value column (not the key column) to the calculator
                double val = aggFunc(kvp.Value, valueCol);

                resultCol.AppendObject(val);
            }

            return new DataFrame(new[] { keyCol, resultCol });
        }
    }
}