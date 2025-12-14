using System.Text;
using LeichtFrame.Core;

namespace LeichtFrame.IO.Tests
{
    public class CsvReaderTests
    {
        [Fact]
        public void Read_Parses_Simple_Csv_With_Header()
        {
            var csv = "Id,Name,Score\n1,Alice,99.5\n2,Bob,80.0";
            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csv));

            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Name", typeof(string)),
                new ColumnDefinition("Score", typeof(double))
            });

            var df = CsvReader.Read(stream, schema);

            Assert.Equal(2, df.RowCount);
            Assert.Equal(1, df["Id"].Get<int>(0));
            Assert.Equal("Alice", df["Name"].Get<string>(0));
            Assert.Equal(99.5, df["Score"].Get<double>(0));
        }

        [Fact]
        public void Read_Handles_Nulls()
        {
            var csv = "Val\n100\n\n200";
            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(csv));

            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Val", typeof(int), IsNullable: true)
            });

            var df = CsvReader.Read(stream, schema);

            Assert.Equal(3, df.RowCount);
            Assert.Equal(100, df["Val"].Get<int>(0));
            Assert.True(df["Val"].IsNull(1));
            Assert.Equal(200, df["Val"].Get<int>(2));
        }

        [Fact]
        public void InferSchema_Detects_Int_Double_And_String()
        {
            string csvFile = Path.GetTempFileName();
            File.WriteAllText(csvFile, "Age,Weight,Name\n25,80.5,Alice\n30,90,Bob"); // 90 is int, 80.5 double

            try
            {
                var schema = CsvReader.InferSchema(csvFile);

                Assert.Equal(3, schema.Columns.Count);

                // Age: Only ints -> int
                Assert.Equal(typeof(int), schema.GetColumnType("Age"));

                // Weight: Mixed double and int -> double (Promotion)
                Assert.Equal(typeof(double), schema.GetColumnType("Weight"));

                // Name: String
                Assert.Equal(typeof(string), schema.GetColumnType("Name"));
            }
            finally
            {
                File.Delete(csvFile);
            }
        }

        [Fact]
        public void InferSchema_Detects_Nullability()
        {
            string csvFile = Path.GetTempFileName();
            File.WriteAllText(csvFile, "Val\n100\n\n200");

            try
            {
                var schema = CsvReader.InferSchema(csvFile);
                Assert.Equal(typeof(int), schema.GetColumnType("Val"));
                Assert.True(schema.Columns[0].IsNullable);
            }
            finally
            {
                File.Delete(csvFile);
            }
        }

        [Fact]
        public void InferSchema_Fallbacks_To_String_On_Conflict()
        {
            string csvFile = Path.GetTempFileName();
            File.WriteAllText(csvFile, "Mixed\n100\nHello"); // Int then String

            try
            {
                var schema = CsvReader.InferSchema(csvFile);
                Assert.Equal(typeof(string), schema.GetColumnType("Mixed"));
            }
            finally
            {
                File.Delete(csvFile);
            }
        }

        private class ProductCsv
        {
            public int Id { get; set; }
            public string Name { get; set; } = "";
            public double Price { get; set; }
        }

        [Fact]
        public void Read_Generic_Map_To_POCO_Schema_Correctly()
        {
            // Arrange
            var csv = "Id,Name,Price\n10,Laptop,999.99\n20,Mouse,19.50";
            using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(csv));

            // Act: The "Gold Standard" Call
            var df = CsvReader.Read<ProductCsv>(stream);

            // Assert
            Assert.Equal(2, df.RowCount);

            // Verify Schema was inferred from POCO
            Assert.Equal(typeof(int), df.GetColumnType("Id"));
            Assert.Equal(typeof(string), df.GetColumnType("Name"));
            Assert.Equal(typeof(double), df.GetColumnType("Price"));

            // Verify Data
            Assert.Equal(10, df["Id"].Get<int>(0));
            Assert.Equal("Laptop", df["Name"].Get<string>(0));
            Assert.Equal(999.99, df["Price"].Get<double>(0));
        }

        [Fact]
        public void ReadBatches_Splits_Data_Correctly()
        {
            // Arrange: Create CSV with 10 rows
            var sb = new StringBuilder();
            sb.AppendLine("Id,Val");
            for (int i = 0; i < 10; i++)
            {
                sb.AppendLine($"{i},{i * 10}");
            }

            string tempFile = Path.GetTempFileName();
            File.WriteAllText(tempFile, sb.ToString());

            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Val", typeof(int))
            });

            try
            {
                // Act: Read in batches of size 3
                // Expectation: 4 Batches (3, 3, 3, 1 rows)
                var batches = CsvReader.ReadBatches(tempFile, schema, batchSize: 3).ToList();

                // Assert
                Assert.Equal(4, batches.Count);

                // Verify Batch 1
                Assert.Equal(3, batches[0].RowCount);
                Assert.Equal(0, batches[0]["Id"].Get<int>(0));
                Assert.Equal(2, batches[0]["Id"].Get<int>(2));

                // Verify Batch 4 (Last one containing remainder)
                Assert.Equal(1, batches[3].RowCount);
                Assert.Equal(9, batches[3]["Id"].Get<int>(0));
            }
            finally
            {
                if (File.Exists(tempFile)) File.Delete(tempFile);
            }
        }

        [Fact]
        public void Read_Process_Large_File_Correctly_Using_Parallel_Logic()
        {
            // The logic switches to Parallel processing if chunk size (50k) is reached.
            // We generate 55,000 rows to force at least one chunk + remainder.
            int rows = 55_000;
            var sb = new StringBuilder();
            sb.AppendLine("Id,Value");
            for (int i = 0; i < rows; i++)
            {
                sb.AppendLine($"{i},{i * 0.5}");
            }

            string path = Path.GetTempFileName();
            File.WriteAllText(path, sb.ToString());

            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Value", typeof(double))
            });

            try
            {
                // Act: This will internally use ProcessChunkParallel
                var df = CsvReader.Read(path, schema);

                // Assert
                Assert.Equal(rows, df.RowCount);

                // Spot check first, boundary, last
                Assert.Equal(0, df["Id"].Get<int>(0));
                Assert.Equal(50_000, df["Id"].Get<int>(50_000));
                Assert.Equal(54_999, df["Id"].Get<int>(54_999));

                Assert.Equal(0.0, df["Value"].Get<double>(0));
            }
            finally
            {
                if (File.Exists(path)) File.Delete(path);
            }
        }
    }
}