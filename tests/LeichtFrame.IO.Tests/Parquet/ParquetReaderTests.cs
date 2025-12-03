using LeichtFrame.Core;
using Parquet;
using Parquet.Schema;
using Parquet.Data;

namespace LeichtFrame.IO.Tests
{
    public class ParquetReaderTests
    {
        private async Task<string> CreateTempParquetFile()
        {
            string path = Path.GetTempFileName();

            // Define Parquet Schema manually
            var schema = new ParquetSchema(
                new DataField<int>("Id"),
                new DataField<double?>("Value"), // Nullable
                new DataField<string>("Name")
            );

            // Columns
            var ids = new int[] { 1, 2 };
            var values = new double?[] { 10.5, null };
            var names = new string[] { "Alice", "Bob" };

            using var stream = File.OpenWrite(path);
            using var writer = await Parquet.ParquetWriter.CreateAsync(schema, stream);
            using var groupWriter = writer.CreateRowGroup();

            await groupWriter.WriteColumnAsync(new DataColumn(schema.DataFields[0], ids));
            await groupWriter.WriteColumnAsync(new DataColumn(schema.DataFields[1], values));
            await groupWriter.WriteColumnAsync(new DataColumn(schema.DataFields[2], names));

            return path;
        }

        [Fact]
        public async Task Read_Loads_Parquet_File_Correctly()
        {
            string path = await CreateTempParquetFile();

            try
            {
                // Act
                var df = ParquetReader.Read(path);

                // Assert Schema
                Assert.Equal(2, df.RowCount);
                Assert.Equal(3, df.ColumnCount);
                Assert.True(df.HasColumn("Id"));
                Assert.True(df.HasColumn("Value"));

                // Assert Data
                Assert.Equal(1, df["Id"].Get<int>(0));
                Assert.Equal(10.5, df["Value"].Get<double>(0));
                Assert.Equal("Alice", df["Name"].Get<string>(0));

                // Assert Null Handling
                Assert.Equal(2, df["Id"].Get<int>(1));
                Assert.True(df["Value"].IsNull(1)); // Should be null
                Assert.Equal("Bob", df["Name"].Get<string>(1));
            }
            finally
            {
                File.Delete(path);
            }
        }
    }
}