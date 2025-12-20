using LeichtFrame.Core.Plans;
using LeichtFrame.Core.Expressions;

namespace LeichtFrame.Core.Execution
{
    /// <summary>
    /// Translates a Logical Plan into physical execution steps and produces a materialized DataFrame.
    /// </summary>
    public class PhysicalPlanner
    {
        /// <summary>
        /// Executes the logical plan.
        /// </summary>
        /// <param name="plan">The logical plan to execute.</param>
        /// <returns>The resulting materialized DataFrame.</returns>
        public DataFrame Execute(LogicalPlan plan)
        {
            return plan switch
            {
                DataFrameScan scan => scan.Source,
                Filter filter => ExecuteFilter(filter),
                Projection proj => ExecuteProjection(proj),
                Aggregate agg => ExecuteAggregate(agg),
                Join join => ExecuteJoin(join),
                Sort sort => ExecuteSort(sort),
                _ => throw new NotImplementedException($"Unknown plan node: {plan.GetType().Name}")
            };
        }

        private DataFrame ExecuteFilter(Filter node)
        {
            var inputDf = Execute(node.Input);

            if (node.Predicate is BinaryExpr bin && bin.Left is ColExpr c && bin.Right is LitExpr l)
            {
                var op = MapOp(bin.Op);
                if (l.Value is int iVal) return inputDf.WhereVec(c.Name, op, iVal);
                if (l.Value is double dVal) return inputDf.WhereVec(c.Name, op, dVal);
            }

            throw new NotImplementedException("Complex filters require expression compilation. Please use simple comparisons.");
        }

        private DataFrame ExecuteProjection(Projection node)
        {
            var inputDf = Execute(node.Input);

            var newColumns = new List<IColumn>();
            foreach (var expr in node.Expressions)
            {
                var resultCol = Evaluator.Evaluate(expr, inputDf);

                string targetName = expr is AliasExpr a ? a.Alias : resultCol.Name;

                if (resultCol.Name != targetName)
                {
                    resultCol = RenameColumn(resultCol, targetName);
                }

                newColumns.Add(resultCol);
            }

            return new DataFrame(newColumns);
        }

        private IColumn RenameColumn(IColumn col, string newName)
        {
            var newCol = ColumnFactory.Create(newName, col.DataType, col.Length, col.IsNullable);

            for (int i = 0; i < col.Length; i++)
            {
                if (col.IsNullable && col.IsNull(i))
                {
                    newCol.AppendObject(null);
                }
                else
                {
                    newCol.AppendObject(col.GetValue(i));
                }
            }

            return newCol;
        }

        private CompareOp MapOp(BinaryOp op)
        {
            return op switch
            {
                BinaryOp.Equal => CompareOp.Equal,
                BinaryOp.NotEqual => CompareOp.NotEqual,
                BinaryOp.GreaterThan => CompareOp.GreaterThan,
                BinaryOp.GreaterThanOrEqual => CompareOp.GreaterThanOrEqual,
                BinaryOp.LessThan => CompareOp.LessThan,
                BinaryOp.LessThanOrEqual => CompareOp.LessThanOrEqual,
                _ => throw new NotSupportedException($"Operator {op} not supported in filter.")
            };
        }

        private DataFrame ExecuteAggregate(Aggregate node)
        {
            var inputDf = Execute(node.Input);

            if (node.GroupExprs.Count != 1 || node.GroupExprs[0] is not ColExpr groupColExpr)
                throw new NotImplementedException("Currently only single-column grouping is supported in PhysicalPlanner.");

            string groupColName = groupColExpr.Name;

            using var groupedDf = inputDf.GroupBy(groupColName);

            var aggDefs = new List<AggregationDef>();

            foreach (var expr in node.AggExprs)
            {
                string targetName = expr is AliasExpr a ? a.Alias : null!;
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

            return groupedDf.Aggregate(aggDefs.ToArray());
        }

        private DataFrame ExecuteJoin(Join node)
        {
            var leftDf = Execute(node.Left);
            var rightDf = Execute(node.Right);

            return leftDf.Join(rightDf, node.LeftOn, node.JoinType);
        }

        private DataFrame ExecuteSort(Sort node)
        {
            var df = Execute(node.Input);
            return node.Ascending ? df.OrderBy(node.ColumnName) : df.OrderByDescending(node.ColumnName);
        }
    }
}