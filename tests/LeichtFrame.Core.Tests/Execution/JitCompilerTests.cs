using LeichtFrame.Core.Execution.Compilation;
using static LeichtFrame.Core.Expressions.F;

namespace LeichtFrame.Core.Tests.Execution
{
    public class JitCompilerTests
    {
        [Fact]
        public void Compiler_Generates_Correct_Addition_Logic()
        {
            // Expr: A + B
            var expr = Col("A") + Col("B");
            var mapping = new Dictionary<string, int> { { "A", 0 }, { "B", 1 } };
            var kernel = ExpressionCompiler.CompileDouble(expr, mapping);

            // Data
            int len = 3;
            double[] res = new double[len];
            double[][] inputs = new double[][]
            {
                new[] { 1.0, 2.0, 3.0 }, // A
                new[] { 10.0, 20.0, 30.0 } // B
            };

            // Execute
            kernel(len, res, inputs);

            // Assert
            Assert.Equal(11.0, res[0]);
            Assert.Equal(22.0, res[1]);
            Assert.Equal(33.0, res[2]);
        }

        [Fact]
        public void Compiler_Handles_Literals_And_Precedence()
        {
            // Expr: A * 2.0 + 5.0
            var expr = (Col("A") * 2.0) + 5.0;
            var mapping = new Dictionary<string, int> { { "A", 0 } };
            var kernel = ExpressionCompiler.CompileDouble(expr, mapping);

            int len = 2;
            double[] res = new double[len];
            double[][] inputs = new double[][]
            {
                new[] { 10.0, 5.0 } // A
            };

            kernel(len, res, inputs);

            // (10 * 2) + 5 = 25
            Assert.Equal(25.0, res[0]);
            // (5 * 2) + 5 = 15
            Assert.Equal(15.0, res[1]);
        }

        [Fact]
        public void Compiler_Handles_Complex_Tree()
        {
            // Expr: (A + B) * (A - B)
            var expr = (Col("A") + Col("B")) * (Col("A") - Col("B"));
            var mapping = new Dictionary<string, int> { { "A", 0 }, { "B", 1 } };

            var kernel = ExpressionCompiler.CompileDouble(expr, mapping);

            int len = 1;
            double[] res = new double[len];
            double[][] inputs = new double[][]
            {
                new[] { 5.0 }, // A
                new[] { 3.0 }  // B
            };

            kernel(len, res, inputs);

            // (5+3) * (5-3) = 8 * 2 = 16
            Assert.Equal(16.0, res[0]);
        }

        [Fact]
        public void Compiler_Generates_Correct_Int_Logic()
        {
            var expr = Col("A") + Col("B") * 2;
            var mapping = new Dictionary<string, int> { { "A", 0 }, { "B", 1 } };

            var kernel = ExpressionCompiler.CompileInt(expr, mapping);

            int len = 2;
            int[] res = new int[len];
            int[][] inputs = new int[][]
            {
                new[] { 10, 20 }, // A
                new[] { 5, 3 }    // B
            };

            kernel(len, res, inputs);

            // 10 + (5*2) = 20
            Assert.Equal(20, res[0]);
            // 20 + (3*2) = 26
            Assert.Equal(26, res[1]);
        }
    }
}