using static LeichtFrame.Core.Expressions.F;

namespace LeichtFrame.Core.Tests.Execution
{
    public class EvaluatorIntegrationTests
    {
        [Fact]
        public void Evaluator_Uses_JIT_For_Complex_Double_Arithmetic()
        {
            // Arrange
            var df = DataFrame.FromObjects(new[]
            {
                new { Price = 100.0, Discount = 0.1, Tax = 0.19 },
                new { Price = 50.0,  Discount = 0.0, Tax = 0.07 }
            });

            // Act
            var result = df.Lazy()
                           .Select(
                                (Col("Price") * (1.0 - Col("Discount")) * (1.0 + Col("Tax"))).As("FinalPrice")
                           )
                           .Collect();

            // Assert
            var col = result["FinalPrice"];

            Assert.Equal(107.1, col.Get<double>(0), precision: 5);

            Assert.Equal(53.5, col.Get<double>(1), precision: 5);
        }

        [Fact]
        public void Evaluator_Fallback_To_Recursive_For_Ints()
        {
            var df = DataFrame.FromObjects(new[]
            {
                new { A = 10, B = 5 }
            });

            var result = df.Lazy()
                           .Select((Col("A") + Col("B") * 2).As("Res"))
                           .Collect();

            Assert.Equal(20, result["Res"].Get<int>(0));
        }
    }
}