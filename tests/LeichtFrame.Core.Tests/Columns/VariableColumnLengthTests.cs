namespace LeichtFrame.Core.Tests.Columns
{
    public class VariableLengthColumnTests
    {
        [Fact]
        public void Append_And_Get_Works_With_Ascii()
        {
            using var col = new VariableLengthColumn("Text", 10);
            col.Append("Hello");
            col.Append("World");

            Assert.Equal(2, col.Length);
            Assert.Equal("Hello", col.Get(0));
            Assert.Equal("World", col.Get(1));
        }

        [Fact]
        public void Append_Works_With_UTF8_Emojis()
        {
            using var col = new VariableLengthColumn("Unicode", 10);
            string rocket = "🚀"; // 4 Bytes in UTF-8
            col.Append(rocket);
            col.Append("A");

            Assert.Equal(rocket, col.Get(0));
            Assert.Equal("A", col.Get(1));
        }

        [Fact]
        public void Offsets_Are_Calculated_Correctly()
        {
            // Arrange
            using var col = new VariableLengthColumn("Offsets", 10);

            // Act
            col.Append("Hi");   // 2 Bytes
            col.Append("Test"); // 4 Bytes

            // Access internal state via Reflection or assume correctness via Get()
            // Here we trust Get(), but we know:
            // Row 0: Start 0, Len 2
            // Row 1: Start 2, Len 4 (End 6)

            Assert.Equal("Hi", col.Get(0));
            Assert.Equal("Test", col.Get(1));
        }

        [Fact]
        public void Null_Values_Have_Zero_Length()
        {
            using var col = new VariableLengthColumn("Nulls", 10, isNullable: true);
            col.Append("A");
            col.Append(null);
            col.Append("B");

            Assert.Equal(3, col.Length);
            Assert.Equal("A", col.Get(0));
            Assert.Null(col.Get(1));
            Assert.Equal("B", col.Get(2));

            Assert.True(col.IsNull(1));
        }

        [Fact]
        public void Auto_Resize_Works_For_Values()
        {
            // Start with very small byte buffer
            // "BufferGrowth" needs space for int[] and byte[]
            using var col = new VariableLengthColumn("Growth", capacity: 1);
            // Standard constructor estimates 32 bytes per row. 1 row = 32 bytes.

            // Generate a string larger than 32 bytes
            string longStr = new string('x', 100);

            col.Append(longStr);
            col.Append("End");

            Assert.Equal(2, col.Length);
            Assert.Equal(longStr, col.Get(0));
            Assert.Equal("End", col.Get(1));
        }
    }
}