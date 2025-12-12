using LeichtFrame.Core;

namespace LeichtFrame.Core.Tests.DataFrames.Operations
{
    public class ArithmeticTests
    {
        [Fact]
        public void Int_Column_Addition_Works()
        {
            using var c1 = new IntColumn("A", 10);
            using var c2 = new IntColumn("B", 10);
            c1.Append(10); c2.Append(5);
            c1.Append(20); c2.Append(5);

            using var res = c1 + c2;

            Assert.Equal(15, res.Get(0));
            Assert.Equal(25, res.Get(1));
        }

        [Fact]
        public void Double_Column_Multiplication_With_Nulls()
        {
            using var c1 = new DoubleColumn("Price", 10, isNullable: true);
            using var c2 = new DoubleColumn("Qty", 10, isNullable: true);

            c1.Append(10.0); c2.Append(2.0); // 20
            c1.Append(10.0); c2.Append(null); // null
            c1.Append(null); c2.Append(5.0); // null

            using var res = c1 * c2;

            Assert.Equal(20.0, res.Get(0));
            Assert.True(res.IsNull(1));
            Assert.True(res.IsNull(2));
        }

        [Fact]
        public void Scalar_Operations_Work()
        {
            using var c1 = new IntColumn("A", 10);
            c1.Append(10);

            using var res = c1 * 2;

            Assert.Equal(20, res.Get(0));
        }
    }
}