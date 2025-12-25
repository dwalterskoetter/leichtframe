using LeichtFrame.Core.Operations.Join;
using LeichtFrame.Core.Plans;

namespace LeichtFrame.Core.Execution.Materialization.Executors
{
    internal class JoinExecutor : IPhysicalExecutor
    {
        private readonly Join _node;
        private readonly IPhysicalExecutor _left;
        private readonly IPhysicalExecutor _right;

        public JoinExecutor(Join node, IPhysicalExecutor left, IPhysicalExecutor right)
        {
            _node = node;
            _left = left;
            _right = right;
        }

        public DataFrame Execute()
        {
            var leftDf = _left.Execute();
            var rightDf = _right.Execute();
            return DataFrameJoinExtensions.Join(leftDf, rightDf, _node.LeftOn, _node.JoinType);
        }
    }
}