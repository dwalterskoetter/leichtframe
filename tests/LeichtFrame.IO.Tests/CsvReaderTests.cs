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
    }
}