namespace LeichtFrame.Core
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
    /// <param name="SourceColumn">The name of the column to aggregate.</param>
    /// <param name="Operation">The operation to perform.</param>
    /// <param name="TargetName">The name of the resulting column.</param>
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
}