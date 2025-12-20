using LeichtFrame.Core.Expressions;

namespace LeichtFrame.Core.Execution
{
    internal static class Evaluator
    {
        public static IColumn Evaluate(Expr expr, DataFrame df)
        {
            return expr switch
            {
                ColExpr c => df[c.Name],
                BinaryExpr b => EvaluateBinary(b, df),
                LitExpr l => CreateLiteralColumn(l, df.RowCount),
                AliasExpr a => Evaluate(a.Child, df),
                _ => throw new NotImplementedException($"Expr type {expr.GetType().Name} not supported")
            };
        }

        private static IColumn EvaluateBinary(BinaryExpr b, DataFrame df)
        {
            var left = Evaluate(b.Left, df);
            var right = Evaluate(b.Right, df);

            // 1. Dispatch INT
            if (left is IntColumn lInt && right is IntColumn rInt)
            {
                return b.Op switch
                {
                    BinaryOp.Add => lInt + rInt,
                    BinaryOp.Subtract => lInt - rInt,
                    BinaryOp.Multiply => lInt * rInt,
                    BinaryOp.Divide => lInt / rInt,
                    _ => throw new NotSupportedException($"Op {b.Op} not supported for Int")
                };
            }

            // 2. Dispatch DOUBLE
            if (left is DoubleColumn lDbl && right is DoubleColumn rDbl)
            {
                return b.Op switch
                {
                    BinaryOp.Add => lDbl + rDbl,
                    BinaryOp.Subtract => lDbl - rDbl,
                    BinaryOp.Multiply => lDbl * rDbl,
                    BinaryOp.Divide => lDbl / rDbl,
                    _ => throw new NotSupportedException($"Op {b.Op} not supported for Double")
                };
            }

            // 3. Dispatch MIXED (Int + Double) -> Cast Int to Double
            if (left is IntColumn lI && right is DoubleColumn rD)
            {
                var lAsDbl = ConvertIntToDouble(lI);
                return EvaluateBinary(new BinaryExpr(new LitExpr(null), b.Op, new LitExpr(null)), df);
            }

            throw new NotSupportedException($"Type mismatch: {left.DataType.Name} vs {right.DataType.Name}");
        }

        private static DoubleColumn ConvertIntToDouble(IntColumn ic)
        {
            var dc = new DoubleColumn(ic.Name, ic.Length, ic.IsNullable);
            for (int i = 0; i < ic.Length; i++)
            {
                if (ic.IsNull(i)) dc.Append(null);
                else dc.Append((double)ic.Get(i));
            }
            return dc;
        }

        private static IColumn CreateLiteralColumn(LitExpr l, int rowCount)
        {
            if (l.Value == null)
            {
                var col = new IntColumn("Lit", rowCount, true);
                for (int i = 0; i < rowCount; i++) col.SetNull(i);
                return col;
            }

            if (l.Value is int iVal)
            {
                var col = new IntColumn("Lit", rowCount);
                for (int i = 0; i < rowCount; i++) col.Append(iVal);
                return col;
            }
            if (l.Value is double dVal)
            {
                var col = new DoubleColumn("Lit", rowCount);
                for (int i = 0; i < rowCount; i++) col.Append(dVal);
                return col;
            }
            if (l.Value is string sVal)
            {
                var col = new StringColumn("Lit", rowCount);
                for (int i = 0; i < rowCount; i++) col.Append(sVal);
                return col;
            }
            if (l.Value is bool bVal)
            {
                var col = new BoolColumn("Lit", rowCount);
                for (int i = 0; i < rowCount; i++) col.Append(bVal);
                return col;
            }

            throw new NotImplementedException($"Literal type {l.Value.GetType().Name} not supported.");
        }
    }
}