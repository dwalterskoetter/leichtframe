using System.Linq.Expressions;
using LeichtFrame.Core.Expressions;

namespace LeichtFrame.Core.Execution.Compilation
{
    /// <summary>
    /// Double Kernel
    /// </summary>
    /// <param name="length"></param>
    /// <param name="result"></param>
    /// <param name="inputs"></param>
    public delegate void DoubleKernel(int length, double[] result, double[][] inputs);

    /// <summary>
    /// Int Kernel
    /// </summary>
    /// <param name="length"></param>
    /// <param name="result"></param>
    /// <param name="inputs"></param>
    public delegate void IntKernel(int length, int[] result, int[][] inputs);

    /// <summary>
    /// Expression Compiler for double and int
    /// </summary>
    public static class ExpressionCompiler
    {
        /// <summary>
        /// Compiles double
        /// </summary>
        /// <param name="rootExpr"></param>
        /// <param name="colMapping"></param>
        /// <returns></returns>
        public static DoubleKernel CompileDouble(Expr rootExpr, Dictionary<string, int> colMapping)
        {
            return Compile<DoubleKernel, double>(rootExpr, colMapping);
        }

        /// <summary>
        /// Compiles int
        /// </summary>
        /// <param name="rootExpr"></param>
        /// <param name="colMapping"></param>
        /// <returns></returns>
        public static IntKernel CompileInt(Expr rootExpr, Dictionary<string, int> colMapping)
        {
            return Compile<IntKernel, int>(rootExpr, colMapping);
        }

        private static TKernel Compile<TKernel, T>(Expr rootExpr, Dictionary<string, int> colMapping)
        {
            var typeT = typeof(T);
            var typeArr = typeof(T[]);
            var typeArrArr = typeof(T[][]);

            var paramLength = Expression.Parameter(typeof(int), "length");
            var paramResult = Expression.Parameter(typeArr, "result");
            var paramInputs = Expression.Parameter(typeArrArr, "inputs");

            var varI = Expression.Variable(typeof(int), "i");
            var breakLabel = Expression.Label();

            var calculation = Visit<T>(rootExpr, varI, paramInputs, colMapping);

            var loopBody = Expression.Assign(
                Expression.ArrayAccess(paramResult, varI),
                calculation
            );

            var loop = Expression.Block(
                new[] { varI },
                Expression.Assign(varI, Expression.Constant(0)),
                Expression.Loop(
                    Expression.IfThenElse(
                        Expression.LessThan(varI, paramLength),
                        Expression.Block(
                            loopBody,
                            Expression.PostIncrementAssign(varI)
                        ),
                        Expression.Break(breakLabel)
                    ),
                    breakLabel
                )
            );

            var lambda = Expression.Lambda<TKernel>(loop, paramLength, paramResult, paramInputs);
            return lambda.Compile();
        }

        private static Expression Visit<T>(Expr node, ParameterExpression varI, ParameterExpression paramInputs, Dictionary<string, int> colMapping)
        {
            if (node is BinaryExpr b)
            {
                var left = Visit<T>(b.Left, varI, paramInputs, colMapping);
                var right = Visit<T>(b.Right, varI, paramInputs, colMapping);

                return b.Op switch
                {
                    BinaryOp.Add => Expression.Add(left, right),
                    BinaryOp.Subtract => Expression.Subtract(left, right),
                    BinaryOp.Multiply => Expression.Multiply(left, right),
                    BinaryOp.Divide => Expression.Divide(left, right),
                    _ => throw new NotSupportedException($"Op {b.Op} not supported in JIT")
                };
            }

            if (node is ColExpr c)
            {
                int colIndex = colMapping[c.Name];
                var colArray = Expression.ArrayAccess(paramInputs, Expression.Constant(colIndex));
                return Expression.ArrayAccess(colArray, varI);
            }

            if (node is LitExpr l)
            {
                var val = Convert.ChangeType(l.Value, typeof(T));
                return Expression.Constant(val);
            }

            if (node is AliasExpr a)
            {
                return Visit<T>(a.Child, varI, paramInputs, colMapping);
            }

            throw new NotSupportedException($"Node {node.GetType().Name} not supported in JIT");
        }
    }
}