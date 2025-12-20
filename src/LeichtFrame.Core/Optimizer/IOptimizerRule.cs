using LeichtFrame.Core.Plans;

namespace LeichtFrame.Core.Optimizer
{
    /// <summary>
    /// Defines a rule for transforming a logical plan into an optimized version.
    /// </summary>
    public interface IOptimizerRule
    {
        /// <summary>
        /// Applies the rule to the given logical plan.
        /// </summary>
        /// <param name="plan">The input plan.</param>
        /// <returns>A transformed plan, or the original if no optimization applies.</returns>
        LogicalPlan Apply(LogicalPlan plan);
    }
}