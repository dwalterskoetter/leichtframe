namespace LeichtFrame.Core.Plans
{
    /// <summary>
    /// Represents the abstract base class for a node in the logical query plan.
    /// </summary>
    public abstract record LogicalPlan
    {
        /// <summary>
        /// Gets the child nodes of this plan node.
        /// </summary>
        public abstract LogicalPlan[] Children { get; }

        /// <summary>
        /// Gets the schema produced by this plan node.
        /// </summary>
        public abstract DataFrameSchema OutputSchema { get; }
    }
}