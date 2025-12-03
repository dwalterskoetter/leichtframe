using Apache.Arrow;
using Apache.Arrow.Types;
using LeichtFrame.Core;

namespace LeichtFrame.IO.Tests
{
    public class ArrowConverterTests
    {
        [Fact]
        public void ToDataFrame_Converts_RecordBatch_Correctly()
        {
            // 1. Build Arrow RecordBatch manually
            var schema = new Schema.Builder()
                .Field(f => f.Name("Id").DataType(Int32Type.Default))
                .Field(f => f.Name("Score").DataType(DoubleType.Default))
                .Field(f => f.Name("Name").DataType(StringType.Default))
                .Build();

            int length = 2;

            // Build Arrays
            var idBuilder = new Int32Array.Builder().Append(1).Append(2);
            var scoreBuilder = new DoubleArray.Builder().Append(10.5).AppendNull(); // Contains Null
            var nameBuilder = new StringArray.Builder().Append("Alice").Append("Bob");

            var batch = new RecordBatch(schema, new IArrowArray[]
            {
                idBuilder.Build(),
                scoreBuilder.Build(),
                nameBuilder.Build()
            }, length);

            // 2. Act: Convert to LeichtFrame
            var df = batch.ToDataFrame(); // Extension Method usage

            // 3. Assert
            Assert.Equal(2, df.RowCount);
            Assert.Equal(3, df.ColumnCount);

            // Check Int
            Assert.Equal(1, df["Id"].Get<int>(0));
            Assert.Equal(2, df["Id"].Get<int>(1));

            // Check Double (Nullable)
            Assert.Equal(10.5, df["Score"].Get<double>(0));
            Assert.True(df["Score"].IsNull(1));

            // Check String
            Assert.Equal("Alice", df["Name"].Get<string>(0));
            Assert.Equal("Bob", df["Name"].Get<string>(1));
        }
    }
}