using LeichtFrame.Core.Expressions;
using LeichtFrame.Core.Operations.Transform;
using LeichtFrame.Core.Plans;

namespace LeichtFrame.Core.Execution.Materialization.Executors
{
    internal class ProjectionExecutor : IPhysicalExecutor
    {
        private readonly Projection _node;
        private readonly IPhysicalExecutor _input;

        public ProjectionExecutor(Projection node, IPhysicalExecutor input)
        {
            _node = node;
            _input = input;
        }

        public DataFrame Execute()
        {
            var inputDf = _input.Execute();
            var newColumns = new List<IColumn>();

            foreach (var expr in _node.Expressions)
            {
                var resultCol = Evaluator.Evaluate(expr, inputDf);
                string targetName = expr is AliasExpr a ? a.Alias : resultCol.Name;

                if (resultCol.Name != targetName)
                {
                    resultCol = resultCol.Rename(targetName);
                }
                newColumns.Add(resultCol);
            }
            return new DataFrame(newColumns);
        }
    }
}