using static LeichtFrame.Core.Expressions.F;

namespace LeichtFrame.Core.Tests.Lazy
{
    public class ScalarMathTests
    {
        [Fact]
        public void Scalar_Subtraction_Double_Works_Correctly()
        {
            var df = DataFrame.FromObjects(new[]
            {
                new { Discount = 0.2 },
                new { Discount = 0.0 },
                new { Discount = 1.0 }
            });

            var result = df.Lazy()
                           .Select((Lit(1.0) - Col("Discount")).As("Factor"))
                           .Collect();

            var col = result["Factor"];
            Assert.Equal(0.8, col.Get<double>(0), precision: 5);
            Assert.Equal(1.0, col.Get<double>(1), precision: 5); // 1.0 - 0.0
            Assert.Equal(0.0, col.Get<double>(2), precision: 5); // 1.0 - 1.0
        }

        [Fact]
        public void Scalar_Subtraction_Int_Works_Correctly()
        {
            var df = DataFrame.FromObjects(new[]
            {
                new { Qty = 10 },
                new { Qty = 100 },
                new { Qty = 0 }
            });

            var result = df.Lazy()
                           .Select((Lit(100) - Col("Qty")).As("Remaining"))
                           .Collect();

            var col = result["Remaining"];
            Assert.Equal(90, col.Get<int>(0));  // 100 - 10
            Assert.Equal(0, col.Get<int>(1));   // 100 - 100
            Assert.Equal(100, col.Get<int>(2)); // 100 - 0
        }

        [Fact]
        public void Scalar_Multiplication_Is_Commutative()
        {
            // 2 * Col == Col * 2
            var df = DataFrame.FromObjects(new[] { new { A = 10 } });

            var result = df.Lazy()
                           .Select(
                               (Lit(2) * Col("A")).As("Left"),
                               (Col("A") * 2).As("Right")
                           )
                           .Collect();

            Assert.Equal(20, result["Left"].Get<int>(0));
            Assert.Equal(20, result["Right"].Get<int>(0));
        }

        [Fact]
        public void Scalar_Operation_Handles_Nulls()
        {
            var schema = new DataFrameSchema(new[] { new ColumnDefinition("Val", typeof(double), IsNullable: true) });
            var df = DataFrame.Create(schema, 1);
            ((DoubleColumn)df["Val"]).Append(null);

            var result = df.Lazy()
                           .Select((Lit(10.0) - Col("Val")).As("Res"))
                           .Collect();

            Assert.True(result["Res"].IsNull(0));
        }
    }
}