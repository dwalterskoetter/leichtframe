namespace LeichtFrame.Core.Tests.DataFrameTests
{
    public class AggregationTests
    {
        [Fact]
        public void Sum_Works_For_Int_And_Double()
        {
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Ints", typeof(int)),
                new ColumnDefinition("Doubles", typeof(double))
            });
            var df = DataFrame.Create(schema, 10);

            var intCol = (IntColumn)df["Ints"];
            var dblCol = (DoubleColumn)df["Doubles"];

            intCol.Append(10); intCol.Append(20);
            dblCol.Append(1.5); dblCol.Append(2.5);

            Assert.Equal(30.0, df.Sum("Ints"));
            Assert.Equal(4.0, df.Sum("Doubles"));
        }

        [Fact]
        public void Sum_Ignores_Nulls()
        {
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Val", typeof(int), IsNullable: true)
            });
            var df = DataFrame.Create(schema, 5);
            var col = (IntColumn)df["Val"];

            col.Append(10);
            col.Append(null);
            col.Append(20);

            // 10 + 0 + 20 = 30
            Assert.Equal(30.0, df.Sum("Val"));
        }

        [Fact]
        public void MinMax_Works_Correctly()
        {
            var schema = new DataFrameSchema(new[] { new ColumnDefinition("Vals", typeof(int)) });
            var df = DataFrame.Create(schema, 5);
            var col = (IntColumn)df["Vals"];

            col.Append(5);
            col.Append(100);
            col.Append(-10);

            Assert.Equal(-10.0, df.Min("Vals"));
            Assert.Equal(100.0, df.Max("Vals"));
        }

        [Fact]
        public void Mean_Calculates_Average_Correctly()
        {
            var schema = new DataFrameSchema(new[] { new ColumnDefinition("A", typeof(double)) });
            var df = DataFrame.Create(schema, 5);
            var col = (DoubleColumn)df["A"];

            col.Append(2.0);
            col.Append(4.0);

            Assert.Equal(3.0, df.Mean("A"));
        }

        [Fact]
        public void Aggregation_Throws_On_String()
        {
            var schema = new DataFrameSchema(new[] { new ColumnDefinition("Str", typeof(string)) });
            var df = DataFrame.Create(schema, 1);
            ((StringColumn)df["Str"]).Append("Hello");

            Assert.Throws<NotSupportedException>(() => df.Sum("Str"));
        }
    }
}