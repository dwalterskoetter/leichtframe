using LeichtFrame.Core.Expressions;
using LeichtFrame.Core.Plans;

namespace LeichtFrame.Core
{
    /// <summary>
    /// Represents an intermediate state of a LazyDataFrame after a GroupBy operation.
    /// Allows defining aggregations to finish the logical plan node.
    /// </summary>
    public class GroupedLazyFrame
    {
        private readonly LazyDataFrame _parent;
        private readonly List<Expr> _groupKeys;

        internal GroupedLazyFrame(LazyDataFrame parent, IEnumerable<Expr> groupKeys)
        {
            _parent = parent;
            _groupKeys = groupKeys.ToList();
        }

        /// <summary>
        /// Defines the aggregations to perform on the grouped data.
        /// </summary>
        /// <param name="aggregations">A list of aggregation expressions (e.g. Sum, Count).</param>
        /// <returns>A new LazyDataFrame containing the result.</returns>
        public LazyDataFrame Agg(params Expr[] aggregations)
        {
            var node = new Aggregate(_parent.Plan, _groupKeys, aggregations.ToList());
            return new LazyDataFrame(node);
        }
    }
}