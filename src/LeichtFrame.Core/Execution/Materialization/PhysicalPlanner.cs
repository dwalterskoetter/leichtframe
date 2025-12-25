using LeichtFrame.Core.Execution.Materialization.Executors;
using LeichtFrame.Core.Execution.Materialization.Helpers;
using LeichtFrame.Core.Expressions;
using LeichtFrame.Core.Operations.Aggregate;
using LeichtFrame.Core.Plans;

namespace LeichtFrame.Core.Execution.Materialization
{
    /// <summary>
    /// Translates a Logical Plan into a tree of Physical Executors and runs it.
    /// Acts as the factory and orchestrator for materialization.
    /// </summary>
    public class PhysicalPlanner
    {
        /// <summary>
        /// Executes the logical plan by building and running the physical executor tree.
        /// </summary>
        public DataFrame Execute(LogicalPlan plan)
        {
            var executor = BuildPhysicalPlan(plan);
            return executor.Execute();
        }

        /// <summary>
        /// Recursively builds the physical executor tree.
        /// </summary>
        internal IPhysicalExecutor BuildPhysicalPlan(LogicalPlan plan)
        {
            return plan switch
            {
                DataFrameScan scan => new ScanExecutor(scan),

                Filter filter => new FilterExecutor(filter, BuildPhysicalPlan(filter.Input)),

                Projection proj => new ProjectionExecutor(proj, BuildPhysicalPlan(proj.Input)),

                Aggregate agg => new AggregateExecutor(agg, BuildPhysicalPlan(agg.Input)),

                Sort sort => new SortExecutor(sort, BuildPhysicalPlan(sort.Input)),

                Join join => new JoinExecutor(
                    join,
                    BuildPhysicalPlan(join.Left),
                    BuildPhysicalPlan(join.Right)
                ),

                _ => throw new NotImplementedException($"Unknown plan node: {plan.GetType().Name}")
            };
        }

        /// <summary>
        /// Helper used by PhysicalStreamer. 
        /// Delegates to AggregateHelpers.
        /// </summary>
        internal AggregationDef[] MapAggregations(List<Expr> aggExprs)
        {
            return AggregateHelpers.MapAggregations(aggExprs);
        }
    }
}