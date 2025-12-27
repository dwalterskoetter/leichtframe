using static LeichtFrame.Core.Expressions.F;

namespace LeichtFrame.Core.Tests.Lazy
{
    public class StreamingExecutionTests
    {
        [Fact]
        public void CollectStream_SingleColumn_UsesFastPath()
        {
            var df = new DataFrame(new[] { new IntColumn("Id", 10) });
            ((IntColumn)df["Id"]).Append(1);
            ((IntColumn)df["Id"]).Append(1);

            var stream = df.Lazy()
                           .GroupBy("Id")
                           .Agg(Count().As("Count"))
                           .CollectStream();

            int sum = 0;
            foreach (var row in stream)
            {
                sum += row.Get<int>("Count");
            }
            Assert.Equal(2, sum);
        }

        [Fact]
        public void CollectStream_MultiColumn_DoesNotCrash()
        {
            var c1 = new IntColumn("A", 10);
            var c2 = new IntColumn("B", 10);

            c1.Append(1); c2.Append(10);
            c1.Append(1); c2.Append(10);

            c1.Append(2); c2.Append(20);

            var df = new DataFrame(new[] { c1, c2 });

            var stream = df.Lazy()
                           .GroupBy("A", "B")
                           .Agg(Count().As("Count"))
                           .CollectStream();

            var results = new List<(int A, int B, int Cnt)>();

            foreach (var row in stream)
            {
                int a = row.Get<int>("A");
                int b = row.Get<int>("B");
                int cnt = row.Get<int>("Count");
                results.Add((a, b, cnt));
            }

            Assert.Equal(2, results.Count);
            Assert.Contains(results, r => r.A == 1 && r.B == 10 && r.Cnt == 2);
            Assert.Contains(results, r => r.A == 2 && r.B == 20 && r.Cnt == 1);
        }

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

            var results = new List<(string Grp, double Total)>();

            foreach (var row in stream)
            {
                results.Add((row.Get<string>("Grp"), row.Get<double>("Total")));
            }

            Assert.Equal(2, results.Count);

            var rowX = results.First(r => r.Grp == "X");
            Assert.Equal(30.0, rowX.Total);

            var rowY = results.First(r => r.Grp == "Y");
            Assert.Equal(5.0, rowY.Total);
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