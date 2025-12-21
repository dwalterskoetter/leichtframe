using System.Buffers;
using System.Reflection;
using LeichtFrame.Core.Expressions;
using LeichtFrame.Core.Execution.Compilation;

namespace LeichtFrame.Core.Execution
{
    internal static class Evaluator
    {
        private static readonly Dictionary<Expr, DoubleKernel> _doubleKernelCache = new();
        private static readonly Dictionary<Expr, IntKernel> _intKernelCache = new();

        public static IColumn Evaluate(Expr expr, DataFrame df)
        {
            if (expr is ColExpr c) return df[c.Name];
            if (expr is AliasExpr a) return Evaluate(a.Child, df);
            if (expr is LitExpr l) return CreateLiteralColumn(l, df.RowCount);

            if (expr is BinaryExpr b)
            {
                return EvaluateBinary(b, df);
            }

            throw new NotImplementedException($"Expr type {expr.GetType().Name} not supported");
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

            try
            {
                return EvaluateJit(b, df);
            }
            catch (Exception)
            {
                return EvaluateBinaryRecursiveFallback(b, df);
            }
        }

        private static IColumn EvaluateJit(Expr expr, DataFrame df)
        {
            var inputCols = new List<string>();
            CollectColumns(expr, inputCols);

            bool isDoubleMode = false;

            foreach (var name in inputCols)
            {
                var type = df[name].DataType;

                if (type == typeof(double))
                {
                    isDoubleMode = true;
                }
                else if (type != typeof(int))
                {
                    throw new NotSupportedException("JIT supports only Int/Double columns.");
                }
            }

            return isDoubleMode
                ? EvaluateJitDouble(expr, df, inputCols)
                : EvaluateJitInt(expr, df, inputCols);
        }

        private static IColumn EvaluateJitDouble(Expr expr, DataFrame df, List<string> inputCols)
        {
            var inputArrays = new double[inputCols.Count][];
            var colMapping = new Dictionary<string, int>();

            for (int i = 0; i < inputCols.Count; i++)
            {
                string name = inputCols[i];
                colMapping[name] = i;
                var col = df[name];

                if (col is DoubleColumn dc)
                {
                    inputArrays[i] = dc.Values.ToArray();
                }
                else if (col is IntColumn ic)
                {
                    inputArrays[i] = ic.Values.ToArray().Select(x => (double)x).ToArray();
                }
            }

            if (!_doubleKernelCache.TryGetValue(expr, out var kernel))
            {
                kernel = ExpressionCompiler.CompileDouble(expr, colMapping);
                _doubleKernelCache[expr] = kernel;
            }

            var resultArr = ArrayPool<double>.Shared.Rent(df.RowCount);

            kernel(df.RowCount, resultArr, inputArrays);

            return new DoubleColumn("JIT_Res_Dbl", resultArr, df.RowCount);
        }

        private static IColumn EvaluateJitInt(Expr expr, DataFrame df, List<string> inputCols)
        {
            var inputArrays = new int[inputCols.Count][];
            var colMapping = new Dictionary<string, int>();

            for (int i = 0; i < inputCols.Count; i++)
            {
                string name = inputCols[i];
                colMapping[name] = i;
                var col = df[name];

                if (col is IntColumn ic)
                {
                    inputArrays[i] = ic.Values.ToArray();
                }
                else
                {
                    throw new InvalidOperationException("Unexpected type in Int JIT");
                }
            }

            if (!_intKernelCache.TryGetValue(expr, out var kernel))
            {
                kernel = ExpressionCompiler.CompileInt(expr, colMapping);
                _intKernelCache[expr] = kernel;
            }

            var resultArr = ArrayPool<int>.Shared.Rent(df.RowCount);

            kernel(df.RowCount, resultArr, inputArrays);

            return new IntColumn("JIT_Res_Int", resultArr, df.RowCount);
        }

        private static IColumn EvaluateBinaryRecursiveFallback(BinaryExpr b, DataFrame df)
        {
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

            throw new NotSupportedException($"Type mismatch: {left.DataType.Name} vs {right.DataType.Name}");
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
                        // Trick: 1.0 - col => -1.0 * col + 1.0
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
                        // Trick: 10 - col => -1 * col + 10
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
            if (l.Value is DateTime dtVal)
            {
                var col = new DateTimeColumn("Lit", rowCount);
                for (int i = 0; i < rowCount; i++) col.Append(dtVal);
                return col;
            }

            throw new NotImplementedException($"Literal type {l.Value?.GetType().Name ?? "null"} not supported.");
        }

        private static void CollectColumns(Expr expr, List<string> cols)
        {
            if (expr is ColExpr c && !cols.Contains(c.Name)) cols.Add(c.Name);
            if (expr is BinaryExpr b) { CollectColumns(b.Left, cols); CollectColumns(b.Right, cols); }
            if (expr is AliasExpr a) CollectColumns(a.Child, cols);
        }
    }
}