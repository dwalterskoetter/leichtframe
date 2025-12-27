using FluentAssertions;
using LeichtFrame.Core;
using LeichtFrame.Core.Operations.GroupBy;
using LeichtFrame.Core.Operations.Aggregate;
using Xunit;
using Xunit.Abstractions;

namespace LeichtFrame.Core.Tests.Safety
{
    public class ZeroAllocFuzzTests
    {
        private readonly ITestOutputHelper _output;

        public ZeroAllocFuzzTests(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void Fuzz_IntSwissMap_Stability()
        {
            int iterations = 20;
            var rnd = new Random(123);

            for (int i = 0; i < iterations; i++)
            {
                int rows = rnd.Next(10_000, 200_000);
                int maxVal = rnd.Next(rows, rows * 10);

                using var df = GenerateIntDataFrame(rows, maxVal, rnd);

                using var gdf = df.GroupBy("Val");

                int totalStreamed = 0;
                foreach (var row in gdf.CountStream())
                {
                    totalStreamed += row.Count;
                }

                totalStreamed.Should().Be(rows, $"Iteration {i}: Total streamed rows mismatch");

                if (i % 5 == 0)
                {
                    var linqCount = ((IntColumn)df["Val"]).Values.ToArray().Distinct().Count();
                    gdf.GroupCount.Should().Be(linqCount);
                }
            }
        }

        [Fact]
        public void Fuzz_StringMap_AVX2_Stability()
        {
            int iterations = 20;
            var rnd = new Random(456);

            for (int i = 0; i < iterations; i++)
            {
                int rows = rnd.Next(10_000, 50_000);
                using var df = GenerateStringDataFrame(rows, rnd);

                // Act: GroupBy
                using var gdf = df.GroupBy("Str");

                // Checksum
                int totalStreamed = 0;
                foreach (var row in gdf.CountStream())
                {
                    totalStreamed += row.Count;
                }
                totalStreamed.Should().Be(rows);
            }
        }

        // --- Helpers ---

        private DataFrame GenerateIntDataFrame(int rows, int maxVal, Random rnd)
        {
            var col = new IntColumn("Val", rows, isNullable: false);
            for (int i = 0; i < rows; i++) col.Append(rnd.Next(0, maxVal));
            return new DataFrame(new IColumn[] { col });
        }

        private DataFrame GenerateStringDataFrame(int rows, Random rnd)
        {
            var col = new StringColumn("Str", rows);
            for (int i = 0; i < rows; i++)
            {
                int len = rnd.Next(20, 45);
                col.Append(RandomString(len, rnd));
            }
            return new DataFrame(new IColumn[] { col });
        }

        private string RandomString(int length, Random rnd)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            char[] stringChars = new char[length];
            for (int i = 0; i < length; i++) stringChars[i] = chars[rnd.Next(chars.Length)];
            return new string(stringChars);
        }
    }
}