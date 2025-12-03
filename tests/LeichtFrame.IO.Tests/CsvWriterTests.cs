using System.Text;
using LeichtFrame.Core;

namespace LeichtFrame.IO.Tests
{
    public class CsvWriterTests
    {
        [Fact]
        public void Write_Produces_Valid_String_Format()
        {
            // Arrange
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Price", typeof(double))
            });
            var df = DataFrame.Create(schema, 2);
            ((IntColumn)df["Id"]).Append(1); ((DoubleColumn)df["Price"]).Append(12.5);
            ((IntColumn)df["Id"]).Append(2); ((DoubleColumn)df["Price"]).Append(99.99);

            using var stream = new MemoryStream();

            // Act
            df.WriteCsv(stream);

            // Assert
            string csv = Encoding.UTF8.GetString(stream.ToArray());
            string[] lines = csv.Trim().Split(new[] { "\r\n", "\n" }, StringSplitOptions.None);

            Assert.Equal("Id,Price", lines[0]);
            Assert.Equal("1,12.5", lines[1]); // Verify Invariant Culture (Dot)
            Assert.Equal("2,99.99", lines[2]);
        }

        [Fact]
        public void Roundtrip_Write_Then_Read_Restores_Data()
        {
            // Arrange: Complex Data (Dates, Special Chars needing escaping)
            var original = DataFrame.Create(new DataFrameSchema(new[] {
                new ColumnDefinition("Date", typeof(DateTime)),
                new ColumnDefinition("Note", typeof(string))
            }), 1);

            var now = new DateTime(2023, 10, 05, 12, 30, 00);
            ((DateTimeColumn)original["Date"]).Append(now);
            ((StringColumn)original["Note"]).Append("Hello, World"); // Comma needs escaping!

            string tempFile = Path.GetTempFileName();
            try
            {
                // Act 1: Write
                original.WriteCsv(tempFile);

                // Act 2: Read Back
                var loaded = CsvReader.Read(tempFile, original.Schema);

                // Assert
                Assert.Equal(1, loaded.RowCount);
                Assert.Equal(now, loaded["Date"].Get<DateTime>(0));
                Assert.Equal("Hello, World", loaded["Note"].Get<string>(0)); // Quotes should be gone
            }
            finally
            {
                if (File.Exists(tempFile)) File.Delete(tempFile);
            }
        }
    }
}