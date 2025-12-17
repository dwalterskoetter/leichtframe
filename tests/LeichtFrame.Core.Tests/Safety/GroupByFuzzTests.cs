using LeichtFrame.Core;
using LeichtFrame.Core.Engine;

namespace LeichtFrame.Core.Tests.Safety
{
    public class GroupByFuzzTests
    {
        [Fact]
        public void Fuzz_GroupBy_Parallel_Correctness_Against_LINQ()
        {
            // Wir testen explizit um die Grenze von 100.000 herum und weit darüber
            int[] sizes = { 90_000, 100_000, 100_001, 150_000, 500_000 };
            var rnd = new Random(12345);

            foreach (var size in sizes)
            {
                // 1. Generate Random Data
                // Wir erzeugen absichtlich "Cluster" (wenige Gruppen) und "Scatter" (viele Gruppen)
                int groupCardinality = rnd.Next(1, size / 10); // Zufällige Anzahl an Gruppen

                int[] data = new int[size];
                for (int i = 0; i < size; i++)
                {
                    data[i] = rnd.Next(0, groupCardinality);
                }

                // 2. Run LeichtFrame (Candidate)
                using var col = new IntColumn("Val", size);
                foreach (var val in data) col.Append(val);

                var df = new DataFrame(new[] { col });

                // Trigger Count Aggregation
                var lfResult = df.GroupBy("Val").Count();

                // 3. Run LINQ (Oracle / Truth)
                var linqResult = data
                    .GroupBy(x => x)
                    .ToDictionary(g => g.Key, g => g.Count());

                // 4. Compare
                Assert.Equal(linqResult.Count, lfResult.RowCount);

                var keyCol = (IntColumn)lfResult["Val"];
                var countCol = (IntColumn)lfResult["Count"];

                for (int i = 0; i < lfResult.RowCount; i++)
                {
                    int key = keyCol.Get(i);
                    int count = countCol.Get(i);

                    // Prüfen, ob der Schlüssel im Orakel existiert
                    Assert.True(linqResult.ContainsKey(key), $"LeichtFrame found key {key} which LINQ did not find (Size: {size}).");

                    // Prüfen, ob die Anzahl übereinstimmt
                    Assert.Equal(linqResult[key], count);
                }
            }
        }
    }
}