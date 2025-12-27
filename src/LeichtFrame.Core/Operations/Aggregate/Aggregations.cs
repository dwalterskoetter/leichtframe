namespace LeichtFrame.Core.Operations.Aggregate
{
    /// <summary>
    /// Defines the supported aggregation operations.
    /// </summary>
    public enum AggOp
    {
        /// <summary>Summation of values.</summary>
        Sum,
        /// <summary>Arithmetic mean.</summary>
        Mean,
        /// <summary>Row count.</summary>
        Count,
        /// <summary>Minimum value.</summary>
        Min,
        /// <summary>Maximum value.</summary>
        Max
    }

    /// <summary>
    /// Represents a single aggregation instruction.
    /// </summary>
    public record AggregationDef(string SourceColumn, AggOp Operation, string TargetName);

    /// <summary>
    /// Static helper for building aggregation definitions (Fluent API).
    /// </summary>
    public static class Agg
    {
        /// <summary>Creates a Sum aggregation.</summary>
        public static AggregationDef Sum(string column, string? alias = null)
            => new(column, AggOp.Sum, alias ?? $"sum_{column}");

        /// <summary>Creates a Mean aggregation.</summary>
        public static AggregationDef Mean(string column, string? alias = null)
            => new(column, AggOp.Mean, alias ?? $"avg_{column}");

        /// <summary>Creates a Min aggregation.</summary>
        public static AggregationDef Min(string column, string? alias = null)
            => new(column, AggOp.Min, alias ?? $"min_{column}");

        /// <summary>Creates a Max aggregation.</summary>
        public static AggregationDef Max(string column, string? alias = null)
            => new(column, AggOp.Max, alias ?? $"max_{column}");

        /// <summary>Creates a Count aggregation.</summary>
        public static AggregationDef Count(string? alias = "count")
            => new(string.Empty, AggOp.Count, alias ?? "count");
    }

    /// <summary>
    /// Provides extension methods for calculating global aggregations on DataFrames.
    /// </summary>
    public static class DataFrameAggregationExtensions
    {
        /// <summary>
        /// Extension method for calculating global sum on DataFrames.
        /// </summary>
        /// <param name="df"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static double Sum(this DataFrame df, string columnName)
        {
            var col = df[columnName];
            if (col is DoubleColumn doubleCol) return doubleCol.Sum();
            if (col is IntColumn intCol) return (double)intCol.Sum();
            throw new NotSupportedException($"Sum operation is not supported for column type '{col.DataType.Name}'.");
        }

        /// <summary>
        /// Extension method for calculating global min on DataFrames.
        /// </summary>
        /// <param name="df"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static double Min(this DataFrame df, string columnName)
        {
            var col = df[columnName];
            if (col is DoubleColumn doubleCol) return doubleCol.Min();
            if (col is IntColumn intCol) return (double)intCol.Min();
            throw new NotSupportedException($"Min operation is not supported for column type '{col.DataType.Name}'.");
        }

        /// <summary>
        /// Extension method for calculating global max on DataFrames.
        /// </summary>
        /// <param name="df"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static double Max(this DataFrame df, string columnName)
        {
            var col = df[columnName];
            if (col is DoubleColumn doubleCol) return doubleCol.Max();
            if (col is IntColumn intCol) return (double)intCol.Max();
            throw new NotSupportedException($"Max operation is not supported for column type '{col.DataType.Name}'.");
        }

        /// <summary>
        /// Extension method for calculating global mean on DataFrames.
        /// </summary>
        /// <param name="df"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
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