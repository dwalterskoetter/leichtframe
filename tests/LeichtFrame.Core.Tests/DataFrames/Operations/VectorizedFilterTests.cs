namespace LeichtFrame.Core.Tests.DataFrames.Operations
{
    public class VectorizedFilterTests
    {
        [Fact]
        public void WhereVec_Int_GreaterThan_Works()
        {
            var schema = new DataFrameSchema(new[] { new ColumnDefinition("Val", typeof(int)) });
            var df = DataFrame.Create(schema, 100);
            var col = (IntColumn)df["Val"];

            for (int i = 0; i < 100; i++) col.Append(i);

            var result = df.WhereVec("Val", CompareOp.GreaterThan, 90);

            Assert.Equal(9, result.RowCount);
            Assert.Equal(91, result["Val"].Get<int>(0));
            Assert.Equal(99, result["Val"].Get<int>(8));
        }

        [Fact]
        public void WhereVec_Double_LessThanOrEqual_Works()
        {
            var schema = new DataFrameSchema(new[] { new ColumnDefinition("Num", typeof(double)) });
            var df = DataFrame.Create(schema, 10);
            var col = (DoubleColumn)df["Num"];

            col.Append(1.5);
            col.Append(10.0);
            col.Append(2.5);
            col.Append(3.0);

            var result = df.WhereVec("Num", CompareOp.LessThanOrEqual, 3.0);

            Assert.Equal(3, result.RowCount);
            for (int i = 0; i < result.RowCount; i++)
            {
                Assert.True(result["Num"].Get<double>(i) <= 3.0);
            }
        }

        [Fact]
        public void WhereVec_Ignores_Null_Values()
        {
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Id", typeof(int), IsNullable: true)
            });
            var df = DataFrame.Create(schema, 5);
            var col = (IntColumn)df["Id"];

            col.Append(10);
            col.Append(null);
            col.Append(20);

            var result = df.WhereVec("Id", CompareOp.NotEqual, 5);

            Assert.Equal(2, result.RowCount);
            Assert.Equal(10, result["Id"].Get<int>(0));
            Assert.Equal(20, result["Id"].Get<int>(1));
        }

        [Fact]
        public void WhereVec_Handles_Sparse_Matches_Correctly()
        {
            var schema = new DataFrameSchema(new[] { new ColumnDefinition("A", typeof(int)) });
            var df = DataFrame.Create(schema, 1000);
            var col = (IntColumn)df["A"];

            for (int i = 0; i < 1000; i++) col.Append(0);

            col.SetValue(999, 100);

            var result = df.WhereVec("A", CompareOp.Equal, 100);

            Assert.Equal(1, result.RowCount);
            Assert.Equal(100, result["A"].Get<int>(0));
        }
    }
}