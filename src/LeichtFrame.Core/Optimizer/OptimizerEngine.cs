using LeichtFrame.Core.Plans;

namespace LeichtFrame.Core.Optimizer
{
    /// <summary>
    /// Coordinates the application of optimization rules to a logical plan.
    /// </summary>
    public class OptimizerEngine
    {
        private readonly List<IOptimizerRule> _rules;

        /// <summary>
        /// Initializes a new instance of the <see cref="OptimizerEngine"/> class with default rules.
        /// </summary>
        public OptimizerEngine()
        {
            _rules = new List<IOptimizerRule>
            {
                // Future rules: ConstantFolding, PredicatePushdown, etc.
            };
        }

        /// <summary>
        /// Optimizes the given logical plan by applying all registered rules.
        /// </summary>
        /// <param name="plan">The initial logical plan.</param>
        /// <returns>The optimized logical plan.</returns>
        public LogicalPlan Optimize(LogicalPlan plan)
        {
            var current = plan;
            foreach (var rule in _rules)
            {
                current = rule.Apply(current);
            }
            return current;
        }
    }
}