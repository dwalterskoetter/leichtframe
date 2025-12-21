using LeichtFrame.Core.Expressions;

namespace LeichtFrame.Core.Plans
{
    /// <summary>
    /// Represents a leaf node in the plan that scans an existing DataFrame.
    /// </summary>
    /// <param name="Source">The source DataFrame.</param>
    public record DataFrameScan(DataFrame Source) : LogicalPlan
    {
        /// <inheritdoc/>
        public override LogicalPlan[] Children => Array.Empty<LogicalPlan>();

        /// <inheritdoc/>
        public override DataFrameSchema OutputSchema => Source.Schema;
    }

    /// <summary>
    /// Represents a filtering operation on the dataset.
    /// </summary>
    /// <param name="Input">The input plan node.</param>
    /// <param name="Predicate">The boolean expression used to filter rows.</param>
    public record Filter(LogicalPlan Input, Expr Predicate) : LogicalPlan
    {
        /// <inheritdoc/>
        public override LogicalPlan[] Children => new[] { Input };

        /// <inheritdoc/>
        public override DataFrameSchema OutputSchema => Input.OutputSchema;
    }

    /// <summary>
    /// Represents a projection (select) operation that transforms or selects columns.
    /// </summary>
    /// <param name="Input">The input plan node.</param>
    /// <param name="Expressions">The list of expressions to calculate.</param>
    public record Projection(LogicalPlan Input, List<Expr> Expressions) : LogicalPlan
    {
        /// <inheritdoc/>
        public override LogicalPlan[] Children => new[] { Input };

        /// <inheritdoc/>
        public override DataFrameSchema OutputSchema => Input.OutputSchema;
    }

    /// <summary>
    /// Represents a GroupBy operation followed by aggregations.
    /// </summary>
    public record Aggregate(LogicalPlan Input, List<Expr> GroupExprs, List<Expr> AggExprs) : LogicalPlan
    {
        /// <inheritdoc/>
        public override LogicalPlan[] Children => new[] { Input };

        /// <inheritdoc/>
        // Schema inference skipped for skeleton simplicity
        public override DataFrameSchema OutputSchema => Input.OutputSchema;
    }

    /// <summary>
    /// Represents a Join operation between two plans.
    /// </summary>
    public record Join(LogicalPlan Left, LogicalPlan Right, string LeftOn, string RightOn, JoinType JoinType) : LogicalPlan
    {
        /// <inheritdoc/>
        public override LogicalPlan[] Children => new[] { Left, Right };

        /// <inheritdoc/>
        public override DataFrameSchema OutputSchema => Left.OutputSchema; // Simplified
    }

    /// <summary>
    /// Represents a sorting operation (Multi-Column supported).
    /// </summary>
    public record Sort(LogicalPlan Input, List<(string Name, bool Ascending)> SortColumns) : LogicalPlan
    {
        /// <summary>
        /// Logical plan children
        /// </summary>
        public override LogicalPlan[] Children => new[] { Input };

        /// <summary>
        /// Logical plan output schema
        /// </summary>
        public override DataFrameSchema OutputSchema => Input.OutputSchema;
    }
}