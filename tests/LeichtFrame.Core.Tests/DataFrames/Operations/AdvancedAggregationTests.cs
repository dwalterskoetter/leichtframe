using LeichtFrame.Core;

namespace LeichtFrame.Core.Tests.DataFrames.Operations
{
    public class AdvancedAggregationTests
    {
        [Fact]
        public void Aggregate_Performs_Multiple_Ops_In_Single_Pass()
        {
            var df = DataFrame.FromObjects(new[]
            {
                new { Key = "A", Val = 10 },
                new { Key = "A", Val = 20 },
                new { Key = "B", Val = 5 }
            });

            // Act
            var result = df.GroupBy("Key").Aggregate(
                Agg.Sum("Val", "SumVal"),
                Agg.Count("Cnt"),
                Agg.Mean("Val", "AvgVal")
            );

            // Assert
            Assert.Equal(2, result.RowCount);

            // Check Group A
            var rowA = result.Where(r => r.Get<string>("Key") == "A");
            Assert.Equal(30.0, rowA["SumVal"].Get<double>(0)); // 10+20
            Assert.Equal(2, rowA["Cnt"].Get<int>(0));          // 2 rows
            Assert.Equal(15.0, rowA["AvgVal"].Get<double>(0)); // (10+20)/2
        }

        [Fact]
        public void Aggregate_Handles_All_Nulls_Correctly()
        {
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("G", typeof(int)),
                new ColumnDefinition("V", typeof(int), IsNullable: true)
            });
            var df = DataFrame.Create(schema, 2);
            var g = (IntColumn)df["G"];
            var v = (IntColumn)df["V"];

            g.Append(1); v.Append(null);
            g.Append(1); v.Append(null);

            var result = df.GroupBy("G").Aggregate(
                Agg.Sum("V"),
                Agg.Min("V"),
                Agg.Count()
            );

            Assert.Equal(1, result.RowCount);
            Assert.Equal(0.0, result["sum_V"].Get<double>(0));
            Assert.True(result["min_V"].IsNull(0));
            Assert.Equal(2, result["count"].Get<int>(0));
        }
    }
}