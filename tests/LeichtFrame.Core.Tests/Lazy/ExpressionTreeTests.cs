using LeichtFrame.Core.Expressions;
using static LeichtFrame.Core.Expressions.F;

namespace LeichtFrame.Core.Tests.Lazy
{
    public class ExpressionTreeTests
    {
        [Fact]
        public void Col_Creates_ColExpr()
        {
            var expr = Col("Age");

            Assert.IsType<ColExpr>(expr);
            Assert.Equal("Age", ((ColExpr)expr).Name);
        }

        [Fact]
        public void Operators_Create_BinaryExpr_Tree()
        {
            // Syntax: Col("A") + 5
            var expr = Col("A") + 5;

            Assert.IsType<BinaryExpr>(expr);
            var bin = (BinaryExpr)expr;

            // Check Left: Col("A")
            Assert.IsType<ColExpr>(bin.Left);
            Assert.Equal("A", ((ColExpr)bin.Left).Name);

            // Check Op: Add
            Assert.Equal(BinaryOp.Add, bin.Op);

            // Check Right: Lit(5) (Implicit Conversion)
            Assert.IsType<LitExpr>(bin.Right);
            Assert.Equal(5, ((LitExpr)bin.Right).Value);
        }

        [Fact]
        public void Complex_Tree_Structure_Is_Correct()
        {
            // (A * B) + C
            var expr = (Col("A") * Col("B")) + Col("C");

            var root = Assert.IsType<BinaryExpr>(expr);
            Assert.Equal(BinaryOp.Add, root.Op);

            // Right side is C
            Assert.Equal("C", ((ColExpr)root.Right).Name);

            // Left side is (A * B)
            var left = Assert.IsType<BinaryExpr>(root.Left);
            Assert.Equal(BinaryOp.Multiply, left.Op);
            Assert.Equal("A", ((ColExpr)left.Left).Name);
            Assert.Equal("B", ((ColExpr)left.Right).Name);
        }

        [Fact]
        public void Alias_Wraps_Expression()
        {
            // Col("A").As("B")
            var expr = Col("A").As("B");

            var alias = Assert.IsType<AliasExpr>(expr);
            Assert.Equal("B", alias.Alias);
            Assert.IsType<ColExpr>(alias.Child);
        }
    }
}