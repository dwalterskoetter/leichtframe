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
    }
}