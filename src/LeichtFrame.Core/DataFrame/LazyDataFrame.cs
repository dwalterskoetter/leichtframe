using LeichtFrame.Core.Expressions;
using LeichtFrame.Core.Plans;
using LeichtFrame.Core.Execution;
using LeichtFrame.Core.Optimizer;

namespace LeichtFrame.Core
{
    /// <summary>
    /// Represents a lazy DataFrame builder. 
    /// Operations recorded on this object are not executed until <see cref="Collect"/> is called.
    /// </summary>
    public class LazyDataFrame
    {
        /// <summary>
        /// Gets the current logical plan of transformations.
        /// </summary>
        public LogicalPlan Plan { get; }

        internal LazyDataFrame(LogicalPlan plan)
        {
            Plan = plan;
        }

        /// <summary>
        /// Creates a LazyDataFrame starting from an existing materialized DataFrame.
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <returns>A new LazyDataFrame instance.</returns>
        public static LazyDataFrame From(DataFrame df)
        {
            return new LazyDataFrame(new DataFrameScan(df));
        }

        // --- Lazy Operations ---

        /// <summary>
        /// Filters the rows based on the provided boolean expression.
        /// </summary>
        /// <param name="predicate">The condition expression.</param>
        /// <returns>A new LazyDataFrame with the filter node appended.</returns>
        public LazyDataFrame Where(Expr predicate)
        {
            return new LazyDataFrame(new Filter(Plan, predicate));
        }

        /// <summary>
        /// Projects the DataFrame to the specified expressions.
        /// </summary>
        /// <param name="exprs">The list of expressions to select.</param>
        /// <returns>A new LazyDataFrame with the projection node appended.</returns>
        public LazyDataFrame Select(params Expr[] exprs)
        {
            return new LazyDataFrame(new Projection(Plan, exprs.ToList()));
        }

        /// <summary>
        /// Projects the DataFrame to the specified columns by name.
        /// </summary>
        /// <param name="cols">The names of the columns to select.</param>
        /// <returns>A new LazyDataFrame with the projection node appended.</returns>
        public LazyDataFrame Select(params string[] cols)
        {
            var exprs = cols.Select(c => new ColExpr(c)).Cast<Expr>().ToArray();
            return Select(exprs);
        }

        /// <summary>
        /// Groups the DataFrame by the specified columns and performs aggregations.
        /// </summary>
        /// <param name="groupByCols">The columns to group by.</param>
        /// <param name="aggregations">The aggregations to perform.</param>
        public LazyDataFrame Aggregate(Expr[] groupByCols, Expr[] aggregations)
        {
            return new LazyDataFrame(new Aggregate(Plan, groupByCols.ToList(), aggregations.ToList()));
        }

        /// <summary>
        /// Convenience overload for GroupBy -> Agg syntax.
        /// </summary>
        public LazyDataFrame GroupBy(string groupByCol, params Expr[] aggregations)
        {
            return Aggregate(new[] { F.Col(groupByCol) }, aggregations);
        }

        /// <summary>
        /// Joins this DataFrame with another LazyDataFrame.
        /// </summary>
        public LazyDataFrame Join(LazyDataFrame other, string on, JoinType type = JoinType.Inner)
        {
            // Note: Currently supports only same column name on both sides
            return new LazyDataFrame(new Join(Plan, other.Plan, on, on, type));
        }

        /// <summary>
        /// Sorts the DataFrame.
        /// </summary>
        public LazyDataFrame OrderBy(string column)
        {
            return new LazyDataFrame(new Sort(Plan, column, true));
        }

        /// <summary>
        /// Sorts the DataFrame by the specified column in descending order.
        /// </summary>
        public LazyDataFrame OrderByDescending(string column)
        {
            return new LazyDataFrame(new Sort(Plan, column, false));
        }

        // --- Materialization ---

        /// <summary>
        /// Optimizes and executes the logical plan, returning the result as a materialized DataFrame.
        /// </summary>
        /// <returns>The result DataFrame.</returns>
        public DataFrame Collect()
        {
            // 1. Optimize Plan
            var optimizer = new OptimizerEngine();
            var optimizedPlan = optimizer.Optimize(Plan);

            // 2. Execute Plan
            var physicalPlanner = new PhysicalPlanner();
            return physicalPlanner.Execute(optimizedPlan);
        }
    }
}