namespace LeichtFrame.Core
{
    public static class DataFrameAggregationExtensions
    {
        /// <summary>
        /// Calculates the Sum of a numeric column. Ignores null values.
        /// Returns 0 if column is empty.
        /// </summary>
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
}