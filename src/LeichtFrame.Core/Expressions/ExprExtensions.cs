using LeichtFrame.Core.Expressions;

namespace LeichtFrame.Core
{
    /// <summary>
    /// Provides fluent extension methods for building expressions, aggregations, and aliases.
    /// Enables syntax like: "ColumnName".Sum().As("Total")
    /// </summary>
    public static class ExprExtensions
    {
        // =========================================================
        // Extensions for existing Expr objects
        // =========================================================

        /// <summary>
        /// Creates a summation aggregation expression.
        /// </summary>
        /// <param name="expr">The expression to aggregate.</param>
        public static Expr Sum(this Expr expr) => new AggExpr(AggOpType.Sum, expr);

        /// <summary>
        /// Creates a minimum value aggregation expression.
        /// </summary>
        /// <param name="expr">The expression to aggregate.</param>
        public static Expr Min(this Expr expr) => new AggExpr(AggOpType.Min, expr);

        /// <summary>
        /// Creates a maximum value aggregation expression.
        /// </summary>
        /// <param name="expr">The expression to aggregate.</param>
        public static Expr Max(this Expr expr) => new AggExpr(AggOpType.Max, expr);

        /// <summary>
        /// Creates an arithmetic mean (average) aggregation expression.
        /// </summary>
        /// <param name="expr">The expression to aggregate.</param>
        public static Expr Mean(this Expr expr) => new AggExpr(AggOpType.Mean, expr);

        /// <summary>
        /// Creates a count aggregation expression.
        /// </summary>
        /// <param name="expr">The expression to count.</param>
        public static Expr Count(this Expr expr) => new AggExpr(AggOpType.Count, expr);

        /// <summary>
        /// Aliases the expression with a new name.
        /// </summary>
        /// <param name="expr">The expression to rename.</param>
        /// <param name="alias">The new name for the column.</param>
        public static Expr As(this Expr expr, string alias) => new AliasExpr(expr, alias);

        // =========================================================
        // Extensions for Strings (Implicit Column Reference)
        // =========================================================

        /// <summary>Creates a Sum aggregation on the column specified by name.</summary>
        public static Expr Sum(this string colName) => new ColExpr(colName).Sum();

        /// <summary>Creates a Min aggregation on the column specified by name.</summary>
        public static Expr Min(this string colName) => new ColExpr(colName).Min();

        /// <summary>Creates a Max aggregation on the column specified by name.</summary>
        public static Expr Max(this string colName) => new ColExpr(colName).Max();

        /// <summary>Creates a Mean aggregation on the column specified by name.</summary>
        public static Expr Mean(this string colName) => new ColExpr(colName).Mean();

        /// <summary>
        /// Creates a Count aggregation.
        /// Use "*" to count all rows (equivalent to SQL COUNT(*)).
        /// Use a column name to count non-null values in that column.
        /// </summary>
        public static Expr Count(this string colName)
        {
            if (colName == "*")
            {
                return new AggExpr(AggOpType.Count, new LitExpr(1));
            }
            return new ColExpr(colName).Count();
        }

        /// <summary>
        /// Creates a column reference with an alias (e.g. for Select projections).
        /// Usage: select("OldName".As("NewName"))
        /// </summary>
        public static Expr As(this string colName, string alias) => new ColExpr(colName).As(alias);
    }

    /// <summary>
    /// Static Entry Point for expressions, similar to 'pl' in Polars.
    /// </summary>
    public static class Lf
    {
        /// <summary>
        /// Creates a column reference expression.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        public static Expr Col(string name) => new ColExpr(name);

        /// <summary>
        /// Creates a literal value expression.
        /// </summary>
        /// <param name="val">The value to wrap.</param>
        public static Expr Lit(object val) => new LitExpr(val);
    }
}