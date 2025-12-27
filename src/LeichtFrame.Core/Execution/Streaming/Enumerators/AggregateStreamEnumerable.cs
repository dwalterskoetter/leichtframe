using System.Collections;
using LeichtFrame.Core.Execution.Materialization;
using LeichtFrame.Core.Expressions;
using LeichtFrame.Core.Operations.Aggregate;
using LeichtFrame.Core.Operations.GroupBy;
using LeichtFrame.Core.Plans;

namespace LeichtFrame.Core.Execution.Streaming.Enumerators
{
    internal class AggregateStreamEnumerable : IEnumerable<RowView>
    {
        private readonly Aggregate _node;

        public AggregateStreamEnumerable(Aggregate node)
        {
            _node = node;
        }

        public IEnumerator<RowView> GetEnumerator()
        {
            var inputDf = new PhysicalPlanner().Execute(_node.Input);
            var groupCols = _node.GroupExprs.Cast<ColExpr>().Select(c => c.Name).ToArray();

            var gdf = GroupingOps.GroupBy(inputDf, groupCols);

            // Fast Path Check
            if (_node.AggExprs.Count == 1 && gdf.NativeData != null)
            {
                var expr = _node.AggExprs[0];
                string targetName = expr is AliasExpr a ? a.Alias : "Agg";
                Expr coreExpr = expr is AliasExpr aliasExpr ? aliasExpr.Child : expr;

                if (coreExpr is AggExpr agg)
                {
                    if (agg.Op == AggOpType.Count)
                    {
                        return new FastNativeEnumerator(gdf, groupCols, targetName, AggOpType.Count, null);
                    }
                    else if (agg.Op == AggOpType.Sum && agg.Child is ColExpr c)
                    {
                        var valCol = inputDf[c.Name];
                        return new FastNativeEnumerator(gdf, groupCols, targetName, AggOpType.Sum, valCol);
                    }
                }
            }

            // Fallback / Slow Path
            var planner = new PhysicalPlanner();
            var aggDefs = planner.MapAggregations(_node.AggExprs);
            var materializedResult = gdf.Aggregate(aggDefs);

            gdf.Dispose();

            return new DataFrameEnumerable(materializedResult).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}