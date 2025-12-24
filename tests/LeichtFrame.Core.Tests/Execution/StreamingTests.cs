namespace LeichtFrame.Core.Tests.Execution
{
    public class StreamingTests
    {
        [Fact]
        public void CollectStream_SingleColumn_UsesFastPath()
        {
            var df = new DataFrame(new[] { new IntColumn("Id", 10) });
            ((IntColumn)df["Id"]).Append(1);
            ((IntColumn)df["Id"]).Append(1);

            var stream = df.Lazy()
                           .GroupBy("Id")
                           .Agg(F.Count().As("Count"))
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
                           .Agg(F.Count().As("Count"))
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
    }
}