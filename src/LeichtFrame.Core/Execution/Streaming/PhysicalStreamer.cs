using LeichtFrame.Core.Execution.Materialization;
using LeichtFrame.Core.Execution.Streaming.Enumerators;
using LeichtFrame.Core.Plans;

namespace LeichtFrame.Core.Execution.Streaming
{
    /// <summary>
    /// Executes a Logical Plan in a streaming fashion to minimize memory allocation.
    /// Acts as the main entry point for the streaming engine.
    /// </summary>
    public static class PhysicalStreamer
    {
        /// <summary>
        /// Execute Logical Plan and return a lazy enumerable of RowViews.
        /// </summary>
        public static IEnumerable<RowView> Execute(LogicalPlan plan)
        {
            // Routing based on the top-most node
            if (plan is Aggregate aggNode)
            {
                return new AggregateStreamEnumerable(aggNode);
            }

            var df = new PhysicalPlanner().Execute(plan);
            return new DataFrameEnumerable(df);
        }
    }
}