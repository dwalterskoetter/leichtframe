namespace LeichtFrame.Core.Tests.Lazy
{
    public class StreamingExecutionTests
    {
        [Fact]
        public void CollectStream_FastPath_Count_Returns_Correct_Rows()
        {

            var df = DataFrame.FromObjects(new[]
            {
                new { Id = 1, Grp = "A" },
                new { Id = 2, Grp = "A" },
                new { Id = 3, Grp = "B" }
            });

            var stream = df.Lazy()
                           .GroupBy("Grp")
                           .Agg("*".Count().As("Cnt"))
                           .CollectStream();

            int rowsRead = 0;
            var results = new Dictionary<string, int>();

            foreach (var row in stream)
            {
                rowsRead++;
                string key = row.Get<string>("Grp");
                int count = row.Get<int>("Cnt");
                results[key] = count;
            }

            Assert.Equal(2, rowsRead);
            Assert.Equal(2, results["A"]);
            Assert.Equal(1, results["B"]);
        }

        [Fact]
        public void CollectStream_FallbackPath_Sum_Returns_Correct_Rows()
        {
            var df = DataFrame.FromObjects(new[]
            {
                new { Grp = "X", Val = 10 },
                new { Grp = "X", Val = 20 },
                new { Grp = "Y", Val = 5 }
            });

            var stream = df.Lazy()
                           .GroupBy("Grp")
                           .Agg("Val".Sum().As("Total"))
                           .CollectStream();

            var list = stream.ToList();

            Assert.Equal(2, list.Count);

            var rowX = list.First(r => r.Get<string>("Grp") == "X");
            Assert.Equal(30.0, rowX.Get<double>("Total"));

            var rowY = list.First(r => r.Get<string>("Grp") == "Y");
            Assert.Equal(5.0, rowY.Get<double>("Total"));
        }

        [Fact]
        public void CollectStream_Empty_DataFrame_Yields_Nothing()
        {
            var schema = new DataFrameSchema(new[] { new ColumnDefinition("A", typeof(int)) });
            var df = DataFrame.Create(schema, 0);

            var stream = df.Lazy()
                           .GroupBy("A")
                           .Agg("*".Count().As("C"))
                           .CollectStream();

            Assert.Empty(stream);
        }
    }
}