using LeichtFrame.Core.Execution.Materialization.Helpers; // Zugriff auf Helpers
using LeichtFrame.Core.Expressions;
using LeichtFrame.Core.Operations.Aggregate;
using LeichtFrame.Core.Operations.GroupBy;
using LeichtFrame.Core.Operations.Transform;
using LeichtFrame.Core.Plans;

namespace LeichtFrame.Core.Execution.Materialization.Executors
{
    internal class AggregateExecutor : IPhysicalExecutor
    {
        private readonly Aggregate _node;
        private readonly IPhysicalExecutor _input;

        public AggregateExecutor(Aggregate node, IPhysicalExecutor input)
        {
            _node = node;
            _input = input;
        }

        public DataFrame Execute()
        {
            var inputDf = _input.Execute();

            var colNames = new List<string>();
            foreach (var expr in _node.GroupExprs)
            {
                if (expr is ColExpr c) colNames.Add(c.Name);
                else throw new NotImplementedException("Only column references supported in GroupBy.");
            }

            using var groupedDf = GroupingOps.GroupBy(inputDf, colNames.ToArray());

            // --- Performance Fast Path ---
            if (_node.AggExprs.Count == 1 && colNames.Count == 1)
            {
                var aggExpr = _node.AggExprs[0];
                string targetName = aggExpr is AliasExpr a ? a.Alias : "Count";
                Expr core = aggExpr is AliasExpr alias ? alias.Child : aggExpr;

                if (core is AggExpr ae && ae.Op == AggOpType.Count)
                {
                    var result = groupedDf.Count();
                    if (targetName != "Count")
                    {
                        var countCol = result["Count"].Rename(targetName);
                        var newCols = result.Columns.Where(c => c.Name != "Count").ToList();
                        newCols.Add(countCol);
                        return new DataFrame(newCols);
                    }
                    return result;
                }
            }

            // --- General Path ---
            var aggDefs = AggregateHelpers.MapAggregations(_node.AggExprs);
            return groupedDf.Aggregate(aggDefs);
        }
    }
}