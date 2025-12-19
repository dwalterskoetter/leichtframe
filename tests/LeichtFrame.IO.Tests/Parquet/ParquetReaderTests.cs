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

        [Fact]
        public async Task ReadBatches_Splits_RowGroups_Correctly()
        {
            // Arrange: Create a Parquet file with 2 distinct RowGroups
            string path = Path.GetTempFileName();
            var schema = new ParquetSchema(new DataField<int>("Id"));

            using (var stream = File.OpenWrite(path))
            {
                using var writer = await Parquet.ParquetWriter.CreateAsync(schema, stream);

                // RowGroup 1: IDs 1, 2
                using (var gw1 = writer.CreateRowGroup())
                {
                    await gw1.WriteColumnAsync(new DataColumn(schema.DataFields[0], new int[] { 1, 2 }));
                }

                // RowGroup 2: IDs 3, 4, 5
                using (var gw2 = writer.CreateRowGroup())
                {
                    await gw2.WriteColumnAsync(new DataColumn(schema.DataFields[0], new int[] { 3, 4, 5 }));
                }
            }

            try
            {
                // Act
                // ReadBatches returns IEnumerable<DataFrame>
                var batches = ParquetReader.ReadBatches(path).ToList();

                // Assert
                Assert.Equal(2, batches.Count);

                // Check Batch 1
                Assert.Equal(2, batches[0].RowCount);
                Assert.Equal(1, batches[0]["Id"].Get<int>(0));
                Assert.Equal(2, batches[0]["Id"].Get<int>(1));

                // Check Batch 2
                Assert.Equal(3, batches[1].RowCount);
                Assert.Equal(3, batches[1]["Id"].Get<int>(0));
                Assert.Equal(5, batches[1]["Id"].Get<int>(2));
            }
            finally
            {
                if (File.Exists(path)) File.Delete(path);
            }
        }

        [Fact]
        public async Task ReadAsync_Does_Not_Deadlock()
        {
            string path = Path.GetTempFileName();

            var schema = new Parquet.Schema.ParquetSchema(new Parquet.Schema.DataField<int>("Id"));
            using (var writeStream = File.OpenWrite(path))
            using (var writer = await Parquet.ParquetWriter.CreateAsync(schema, writeStream))
            using (var gw = writer.CreateRowGroup())
            {
                await gw.WriteColumnAsync(new Parquet.Data.DataColumn(schema.DataFields[0], new int[] { 1 }));
            }

            try
            {
                using var stream = File.OpenRead(path);
                var df = await ParquetReader.ReadAsync(stream);

                Assert.NotNull(df);
                Assert.Equal(1, df.RowCount);
            }
            finally
            {
                if (File.Exists(path)) File.Delete(path);
            }
        }
    }
}