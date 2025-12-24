namespace LeichtFrame.Core.Tests.Engine.Algorithms
{
    public class StringConverterTests
    {
        [Fact]
        public void ToCategory_Converts_StringColumn_Correctly()
        {
            using var strCol = new StringColumn("Data", 100, isNullable: true);
            strCol.Append("Apple");
            strCol.Append("Banana");
            strCol.Append("Apple");
            strCol.Append(null);
            strCol.Append("Cherry");
            strCol.Append("Banana");

            using var catCol = StringConverter.ToCategory(strCol);

            Assert.Equal(strCol.Length, catCol.Length);
            Assert.Equal("CategoryColumn", catCol.GetType().Name);

            Assert.Equal("Apple", catCol.Get(0));
            Assert.Equal("Banana", catCol.Get(1));
            Assert.Equal("Apple", catCol.Get(2));
            Assert.Null(catCol.Get(3));
            Assert.True(catCol.IsNull(3));
            Assert.Equal("Cherry", catCol.Get(4));
            Assert.Equal("Banana", catCol.Get(5));

            Assert.Equal(4, catCol.Cardinality);
        }

        [Fact]
        public void ToCategory_Handles_Empty_Strings()
        {
            using var strCol = new StringColumn("Data", 16, isNullable: true);
            strCol.Append("");
            strCol.Append("A");
            strCol.Append("");

            using var catCol = StringConverter.ToCategory(strCol);

            Assert.Equal(3, catCol.Length);
            Assert.Equal("", catCol.Get(0));
            Assert.Equal("A", catCol.Get(1));
            Assert.Equal("", catCol.Get(2));

            Assert.False(catCol.IsNull(0));
        }

        [Fact]
        public void ToCategory_Handles_Large_Input_With_Collisions()
        {
            int n = 2000;
            using var strCol = new StringColumn("Data", n, isNullable: true);
            for (int i = 0; i < n; i++)
            {
                strCol.Append((i % 10).ToString());
            }

            using var catCol = StringConverter.ToCategory(strCol);

            Assert.Equal(n, catCol.Length);

            Assert.Equal(11, catCol.Cardinality);

            Assert.Equal("0", catCol.Get(0));
            Assert.Equal("9", catCol.Get(9));
            Assert.Equal("0", catCol.Get(10));
        }
    }
}