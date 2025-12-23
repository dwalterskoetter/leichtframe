using LeichtFrame.Core.Plans;
using LeichtFrame.Core.Expressions;
using LeichtFrame.Core.Operations.Aggregate;
using LeichtFrame.Core.Operations.Filter;
using LeichtFrame.Core.Operations.Join;
using LeichtFrame.Core.Operations.Sort;
using LeichtFrame.Core.Operations.GroupBy;
using LeichtFrame.Core.Operations.Transform;

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
                if (l.Value is DateTime dtVal) return inputDf.WhereVec(c.Name, op, dtVal);
            }

            throw new NotImplementedException("Complex filters require expression compilation or simple comparisons.");
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
                    resultCol = resultCol.Rename(targetName);
                }

                newColumns.Add(resultCol);
            }

            return new DataFrame(newColumns);
        }

        private DataFrame ExecuteAggregate(Aggregate node)
        {
            var inputDf = Execute(node.Input);

            // 1. Extract Group Columns
            var colNames = new List<string>();
            foreach (var expr in node.GroupExprs)
            {
                if (expr is ColExpr c)
                    colNames.Add(c.Name);
                else
                    throw new NotImplementedException("Only column references supported in GroupBy.");
            }

            // 2. Perform Grouping
            using var groupedDf = GroupingOps.GroupBy(inputDf, colNames.ToArray());

            // --- PERFORMANCE FAST PATH ---
            if (node.AggExprs.Count == 1 && colNames.Count == 1)
            {
                var aggExpr = node.AggExprs[0];
                string targetName = aggExpr is AliasExpr a ? a.Alias : "Count";
                Expr core = aggExpr is AliasExpr alias ? alias.Child : aggExpr;

                if (core is AggExpr ae && ae.Op == AggOpType.Count)
                {
                    var result = groupedDf.Count();

                    if (targetName != "Count")
                    {
                        var countCol = result["Count"];

                        var renamedCol = countCol.Rename(targetName);

                        var newCols = new List<IColumn>();
                        foreach (var c in result.Columns)
                        {
                            if (c.Name == "Count") newCols.Add(renamedCol);
                            else newCols.Add(c);
                        }
                        return new DataFrame(newCols);
                    }

                    return result;
                }
            }
            // -----------------------------

            // 3. General Path
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

            return DataFrameJoinExtensions.Join(leftDf, rightDf, node.LeftOn, node.JoinType);
        }

        private DataFrame ExecuteSort(Sort node)
        {
            var df = Execute(node.Input);

            string[] names = node.SortColumns.Select(x => x.Name).ToArray();
            bool[] ascending = node.SortColumns.Select(x => x.Ascending).ToArray();

            return OrderOps.OrderBy(df, names, ascending);
        }

        /// <summary>
        /// Helper to map logical expressions to physical aggregation definitions.
        /// Exposed internally for PhysicalStreamer.
        /// </summary>
        internal AggregationDef[] MapAggregations(List<Expr> aggExprs)
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

        // --- Helpers ---

        private IColumn RenameColumn(IColumn col, string newName)
        {
            var field = typeof(Column).GetField("<Name>k__BackingField",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);

            if (field != null)
            {
                field.SetValue(col, newName);
                return col;
            }

            Console.WriteLine($"Warnung: Slow Rename für {col.Name} -> {newName}");
            var indices = new int[col.Length];
            for (int i = 0; i < indices.Length; i++) indices[i] = i;

            var newCol = col.CloneSubset(indices);

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
    }
}