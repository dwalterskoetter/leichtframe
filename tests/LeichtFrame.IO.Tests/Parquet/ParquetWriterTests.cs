using LeichtFrame.Core;

namespace LeichtFrame.IO.Tests
{
    public class ParquetWriterTests
    {
        [Fact]
        public void Roundtrip_Write_Then_Read_Preserves_Data()
        {
            // Arrange
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Value", typeof(double), IsNullable: true),
                new ColumnDefinition("Name", typeof(string))
            });

            var original = DataFrame.Create(schema, 2);
            ((IntColumn)original["Id"]).Append(1);
            ((DoubleColumn)original["Value"]).Append(12.34);
            ((StringColumn)original["Name"]).Append("Test");

            ((IntColumn)original["Id"]).Append(2);
            ((DoubleColumn)original["Value"]).Append(null); // Check Nullable
            ((StringColumn)original["Name"]).Append("NullRow");

            string path = Path.GetTempFileName();

            try
            {
                // Act: Write
                original.WriteParquet(path);

                // Act: Read Back (using the Reader we built in C.2.1)
                // Note: Parquet might not preserve IsNullable=false for Int if not specified carefully, 
                // but logic maps nullable based on Parquet schema.
                var loaded = ParquetReader.Read(path);

                // Assert
                Assert.Equal(2, loaded.RowCount);

                Assert.Equal(1, loaded["Id"].Get<int>(0));
                Assert.Equal(12.34, loaded["Value"].Get<double>(0));
                Assert.Equal("Test", loaded["Name"].Get<string>(0));

                Assert.Equal(2, loaded["Id"].Get<int>(1));
                Assert.True(loaded["Value"].IsNull(1));
            }
            finally
            {
                File.Delete(path);
            }
        }
    }
}