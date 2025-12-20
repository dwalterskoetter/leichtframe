namespace LeichtFrame.Core.Operations.Aggregate

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
        /// <exception cref="NotSupportedException">Thrown if the column type is not numeric.</exception>
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
        /// <exception cref="NotSupportedException">Thrown if the column type is not numeric.</exception>
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
        /// <exception cref="NotSupportedException">Thrown if the column type is not numeric.</exception>
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
}