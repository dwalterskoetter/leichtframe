using LeichtFrame.Core.Expressions;
using LeichtFrame.Core.Operations.Aggregate;

namespace LeichtFrame.Core.Execution.Materialization.Helpers
{
    internal static class AggregateHelpers
    {
        public static AggregationDef[] MapAggregations(List<Expr> aggExprs)
        {
            var aggDefs = new List<AggregationDef>();
            foreach (var expr in aggExprs)
            {
                string targetName = expr is AliasExpr a ? a.Alias : "Agg";
                Expr coreExpr = expr is AliasExpr aliasExpr ? aliasExpr.Child : expr;

                if (coreExpr is AggExpr agg)
                {
                    string sourceCol = agg.Child is ColExpr c ? c.Name : "";

                    var def = agg.Op switch
                    {
                        AggOpType.Sum => Agg.Sum(sourceCol, targetName),
                        AggOpType.Min => Agg.Min(sourceCol, targetName),
                        AggOpType.Max => Agg.Max(sourceCol, targetName),
                        AggOpType.Mean => Agg.Mean(sourceCol, targetName),
                        AggOpType.Count => Agg.Count(targetName),
                        _ => throw new NotImplementedException($"Agg op {agg.Op} not supported")
                    };
                    aggDefs.Add(def);
                }
            }
            return aggDefs.ToArray();
        }
    }
}