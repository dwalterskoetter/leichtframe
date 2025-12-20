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
            if (b.Right is LitExpr rightLit)
            {
                var leftCol = Evaluate(b.Left, df);
                return ExecuteScalar(leftCol, rightLit.Value, b.Op, leftIsScalar: false);
            }

            if (b.Left is LitExpr leftLit)
            {
                var rightCol = Evaluate(b.Right, df);
                return ExecuteScalar(rightCol, leftLit.Value, b.Op, leftIsScalar: true);
            }

            var left = Evaluate(b.Left, df);
            var right = Evaluate(b.Right, df);

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

            throw new NotSupportedException($"Type mismatch or unsupported: {left.DataType.Name} vs {right.DataType.Name}");
        }

        private static IColumn ExecuteScalar(IColumn col, object? scalarVal, BinaryOp op, bool leftIsScalar)
        {
            if (scalarVal == null) throw new NotSupportedException("Scalar math with null literals not implemented yet.");

            // === DOUBLE ===
            if (col is DoubleColumn dc)
            {
                double val = Convert.ToDouble(scalarVal);
                if (leftIsScalar)
                {
                    return op switch
                    {
                        BinaryOp.Add => dc + val,
                        BinaryOp.Multiply => dc * val,
                        BinaryOp.Subtract => (dc * -1.0) + val,
                        _ => throw new NotSupportedException($"Scalar Left operation {op} not optimized for Double yet.")
                    };
                }
                else
                {
                    return op switch
                    {
                        BinaryOp.Add => dc + val,
                        BinaryOp.Subtract => dc - val,
                        BinaryOp.Multiply => dc * val,
                        BinaryOp.Divide => dc / val,
                        _ => throw new NotSupportedException($"Op {op} not supported")
                    };
                }
            }

            // === INT ===
            if (col is IntColumn ic)
            {
                int val = Convert.ToInt32(scalarVal);
                if (leftIsScalar)
                {
                    return op switch
                    {
                        BinaryOp.Add => ic + val,
                        BinaryOp.Multiply => ic * val,
                        BinaryOp.Subtract => (ic * -1) + val,
                        _ => throw new NotSupportedException($"Scalar Left operation {op} not optimized for Int yet.")
                    };
                }
                else
                {
                    return op switch
                    {
                        BinaryOp.Add => ic + val,
                        BinaryOp.Subtract => ic - val,
                        BinaryOp.Multiply => ic * val,
                        BinaryOp.Divide => ic / val,
                        _ => throw new NotSupportedException($"Op {op} not supported")
                    };
                }
            }

            throw new NotSupportedException($"Scalar math not supported for column type {col.DataType.Name}");
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