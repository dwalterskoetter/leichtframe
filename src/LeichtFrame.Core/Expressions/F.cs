namespace LeichtFrame.Core.Expressions
{
    /// <summary>
    /// Static entry point for the Fluent API. Provides factory methods for creating expressions.
    /// </summary>
    public static class F
    {
        /// <summary>
        /// Creates a column reference expression.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>A <see cref="ColExpr"/> representing the column.</returns>
        public static Expr Col(string name) => new ColExpr(name);

        /// <summary>
        /// Creates a literal value expression.
        /// </summary>
        /// <param name="val">The value to wrap.</param>
        /// <returns>A <see cref="LitExpr"/> representing the constant.</returns>
        public static Expr Lit(object? val) => new LitExpr(val);

        /// <summary>Creates a Sum aggregation.</summary>
        public static Expr Sum(Expr expr) => new AggExpr(AggOpType.Sum, expr);

        /// <summary>Creates a Min aggregation.</summary>
        public static Expr Min(Expr expr) => new AggExpr(AggOpType.Min, expr);

        /// <summary>Creates a Max aggregation.</summary>
        public static Expr Max(Expr expr) => new AggExpr(AggOpType.Max, expr);

        /// <summary>Creates a Mean aggregation.</summary>
        public static Expr Mean(Expr expr) => new AggExpr(AggOpType.Mean, expr);

        /// <summary>Creates a Count aggregation.</summary>
        public static Expr Count() => new AggExpr(AggOpType.Count, new LitExpr(1));
    }
}