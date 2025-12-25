using LeichtFrame.Core.Expressions;
using LeichtFrame.Core.Plans;
using LeichtFrame.Core.Execution;
using LeichtFrame.Core.Optimizer;
using LeichtFrame.Core.Execution.Materialization;

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
        public LazyDataFrame Where(Expr predicate)
        {
            return new LazyDataFrame(new Filter(Plan, predicate));
        }

        /// <summary>
        /// Projects the DataFrame to the specified expressions.
        /// </summary>
        public LazyDataFrame Select(params Expr[] exprs)
        {
            return new LazyDataFrame(new Projection(Plan, exprs.ToList()));
        }

        /// <summary>
        /// Projects the DataFrame to the specified columns by name.
        /// </summary>
        public LazyDataFrame Select(params string[] cols)
        {
            var exprs = cols.Select(c => new ColExpr(c)).Cast<Expr>().ToArray();
            return Select(exprs);
        }

        /// <summary>
        /// Internal method to create an Aggregate node directly.
        /// </summary>
        public LazyDataFrame Aggregate(Expr[] groupByCols, Expr[] aggregations)
        {
            return new LazyDataFrame(new Aggregate(Plan, groupByCols.ToList(), aggregations.ToList()));
        }

        /// <summary>
        /// Joins this DataFrame with another LazyDataFrame.
        /// </summary>
        public LazyDataFrame Join(LazyDataFrame other, string on, JoinType type = JoinType.Inner)
        {
            return new LazyDataFrame(new Join(Plan, other.Plan, on, on, type));
        }

        /// <summary>
        /// Sorts by one or more columns (Ascending).
        /// </summary>
        public LazyDataFrame OrderBy(params string[] columns)
        {
            var sortDefs = columns.Select(name => (name, true)).ToList();
            return new LazyDataFrame(new Sort(Plan, sortDefs));
        }

        /// <summary>
        /// Sorts by one or more columns (Descending).
        /// </summary>
        public LazyDataFrame OrderByDescending(params string[] columns)
        {
            var sortDefs = columns.Select(name => (name, false)).ToList();
            return new LazyDataFrame(new Sort(Plan, sortDefs));
        }

        // --- Materialization ---

        /// <summary>
        /// Optimizes and executes the logical plan, returning the result as a materialized DataFrame.
        /// </summary>
        public DataFrame Collect()
        {
            var optimizer = new OptimizerEngine();
            var optimizedPlan = optimizer.Optimize(Plan);

            var physicalPlanner = new PhysicalPlanner();
            return physicalPlanner.Execute(optimizedPlan);
        }

        // --- Aggregation ---

        /// <summary>
        /// Groups the DataFrame by the specified columns.
        /// Returns a <see cref="GroupedLazyFrame"/> to define aggregations via .Agg(...).
        /// </summary>
        public GroupedLazyFrame GroupBy(params Expr[] cols)
        {
            return new GroupedLazyFrame(this, cols);
        }

        /// <summary>
        /// Groups the DataFrame by the specified column names.
        /// Returns a <see cref="GroupedLazyFrame"/> to define aggregations via .Agg(...).
        /// </summary>
        public GroupedLazyFrame GroupBy(params string[] cols)
        {
            var exprs = cols.Select(c => new ColExpr(c)).Cast<Expr>();
            return new GroupedLazyFrame(this, exprs);
        }

        // --- Streaming (API) ---

        /// <summary>
        /// Executes the plan and returns a streaming iterator over the results.
        /// This avoids materializing the full result DataFrame in memory, which is ideal for large aggregations.
        /// </summary>
        public IEnumerable<RowView> CollectStream()
        {
            var optimizer = new OptimizerEngine();
            var optimizedPlan = optimizer.Optimize(Plan);

            return PhysicalStreamer.Execute(optimizedPlan);
        }
    }
}