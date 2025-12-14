using System.Text;
using LeichtFrame.Core;

namespace LeichtFrame.IO.Tests.Csv
{
    public class CsvFuzzTests
    {
        [Fact]
        public void Fuzz_Csv_Parallel_And_Batching_Consistency()
        {
            var rnd = new Random(42);
            int iterations = 10;

            for (int i = 0; i < iterations; i++)
            {
                // 1. Random Parameters
                int rowCount = rnd.Next(100, 100_000);
                int batchSize = rnd.Next(1, rowCount * 2);
                bool hasHeader = rnd.NextDouble() > 0.5;

                // 2. Generate CSV File
                string file = Path.GetTempFileName();
                try
                {
                    var sb = new StringBuilder();
                    if (hasHeader) sb.AppendLine("Id,Val,Comment");

                    long expectedSumId = 0;

                    for (int r = 0; r < rowCount; r++)
                    {
                        int id = r;
                        double val = rnd.NextDouble();
                        string comment = $"Text {r}, with \"quotes\"";

                        comment = "\"" + comment.Replace("\"", "\"\"") + "\"";

                        sb.AppendLine($"{id},{val:F4},{comment}");
                        expectedSumId += id;
                    }
                    File.WriteAllText(file, sb.ToString());

                    var options = new CsvReadOptions { HasHeader = hasHeader };
                    var schema = new DataFrameSchema(new[] {
                        new ColumnDefinition("Id", typeof(int)),
                        new ColumnDefinition("Val", typeof(double)),
                        new ColumnDefinition("Comment", typeof(string))
                    });

                    // 3. Test A: Parallel Read (Full Load)
                    var dfFull = CsvReader.Read(file, schema, options);

                    Assert.Equal(rowCount, dfFull.RowCount);

                    Assert.Equal(expectedSumId, ((IntColumn)dfFull["Id"]).Sum());

                    // 4. Test B: Batch Read (Streaming)
                    long batchRowTotal = 0;
                    long batchSumId = 0;
                    int batchCount = 0;

                    foreach (var batch in CsvReader.ReadBatches(file, schema, batchSize, options))
                    {
                        batchRowTotal += batch.RowCount;

                        batchSumId += ((IntColumn)batch["Id"]).Sum();

                        batchCount++;

                        Assert.True(batch.RowCount <= batchSize);
                    }

                    Assert.Equal(rowCount, batchRowTotal);
                    Assert.Equal(expectedSumId, batchSumId);
                }
                finally
                {
                    if (File.Exists(file)) File.Delete(file);
                }
            }
        }
    }
}