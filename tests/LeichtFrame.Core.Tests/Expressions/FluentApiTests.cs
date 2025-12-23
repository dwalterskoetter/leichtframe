namespace LeichtFrame.Core.Tests.Expressions
{
    public class FluentApiTests
    {
        [Fact]
        public void String_Extension_Sum_Creates_Correct_Expression()
        {
            Expr expr = "Salary".Sum();

            var agg = Assert.IsType<AggExpr>(expr);
            Assert.Equal(AggOpType.Sum, agg.Op);

            var col = Assert.IsType<ColExpr>(agg.Child);
            Assert.Equal("Salary", col.Name);
        }

        [Fact]
        public void String_Extension_Count_Star_Creates_Correct_Expression()
        {
            Expr expr = "*".Count();

            var agg = Assert.IsType<AggExpr>(expr);
            Assert.Equal(AggOpType.Count, agg.Op);

            var lit = Assert.IsType<LitExpr>(agg.Child);
            Assert.Equal(1, lit.Value);
        }

        [Fact]
        public void String_Extension_Count_Column_Creates_Correct_Expression()
        {
            Expr expr = "MyCol".Count();

            var agg = Assert.IsType<AggExpr>(expr);
            Assert.Equal(AggOpType.Count, agg.Op);

            var col = Assert.IsType<ColExpr>(agg.Child);
            Assert.Equal("MyCol", col.Name);
        }

        [Fact]
        public void Chaining_As_Works()
        {
            Expr expr = "Price".Mean().As("AvgPrice");

            var alias = Assert.IsType<AliasExpr>(expr);
            Assert.Equal("AvgPrice", alias.Alias);

            var agg = Assert.IsType<AggExpr>(alias.Child);
            Assert.Equal(AggOpType.Mean, agg.Op);
        }

        [Fact]
        public void Expr_Extension_Works_On_Complex_Logic()
        {
            Expr complex = (Lf.Col("A") + Lf.Col("B")).Sum();

            var agg = Assert.IsType<AggExpr>(complex);
            Assert.Equal(AggOpType.Sum, agg.Op);

            var bin = Assert.IsType<BinaryExpr>(agg.Child);
            Assert.Equal(BinaryOp.Add, bin.Op);
        }
    }
}