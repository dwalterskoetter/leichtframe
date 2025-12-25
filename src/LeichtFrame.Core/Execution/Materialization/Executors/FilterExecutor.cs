using LeichtFrame.Core.Execution.Materialization.Helpers; // Zugriff auf Helpers
using LeichtFrame.Core.Expressions;
using LeichtFrame.Core.Operations.Filter;
using LeichtFrame.Core.Plans;

namespace LeichtFrame.Core.Execution.Materialization.Executors
{
    internal class FilterExecutor : IPhysicalExecutor
    {
        private readonly Filter _node;
        private readonly IPhysicalExecutor _input;

        public FilterExecutor(Filter node, IPhysicalExecutor input)
        {
            _node = node;
            _input = input;
        }

        public DataFrame Execute()
        {
            var inputDf = _input.Execute();

            if (_node.Predicate is BinaryExpr bin && bin.Left is ColExpr c && bin.Right is LitExpr l)
            {
                // Nutzung des Helpers
                var op = FilterHelpers.MapOp(bin.Op);

                if (l.Value is int iVal) return inputDf.WhereVec(c.Name, op, iVal);
                if (l.Value is double dVal) return inputDf.WhereVec(c.Name, op, dVal);
                if (l.Value is DateTime dtVal) return inputDf.WhereVec(c.Name, op, dtVal);
            }

            throw new NotImplementedException("Complex filters require expression compilation.");
        }
    }
}