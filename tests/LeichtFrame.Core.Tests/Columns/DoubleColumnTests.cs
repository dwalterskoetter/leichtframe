using Xunit;
using LeichtFrame.Core;

namespace LeichtFrame.Core.Tests.Columns
{
    public class DoubleColumnTests
    {
        [Fact]
        public void Statistical_Helpers_Ignore_Nulls()
        {
            using var col = new DoubleColumn("Stats", 10, isNullable: true);
            col.Append(10.0);
            col.Append(20.0);
            col.Append((double?)null);
            col.Append(5.0);

            Assert.Equal(35.0, col.Sum());
            Assert.Equal(5.0, col.Min());
            Assert.Equal(20.0, col.Max());
        }

        [Fact]
        public void NaN_Distinction_Works()
        {
            // Requirement: NaN = actual NaN, Null = bitmap
            using var col = new DoubleColumn("NaNTest", 10, isNullable: true);

            col.Append(double.NaN);       // Mathematical NaN
            col.Append((double?)null);    // Logical Null

            // Index 0: Not Null, but value is NaN
            Assert.False(col.IsNull(0));
            Assert.True(double.IsNaN(col.GetValue(0)));

            // Index 1: Is Null
            Assert.True(col.IsNull(1));
        }

        [Fact]
        public void Aggregations_Work_On_NonNullable()
        {
            using var col = new DoubleColumn("Strict", 10, isNullable: false);
            col.Append(1.5);
            col.Append(2.5);

            Assert.Equal(4.0, col.Sum());
            Assert.Equal(1.5, col.Min());
            Assert.Equal(2.5, col.Max());
        }
    }
}