namespace LeichtFrame.Core.Tests.DataFrameTests
{
    public class DataFrameFactoryTests
    {
        // POCO for Testing
        private class User
        {
            public int Id { get; set; }
            public string Name { get; set; } = null!;
            public double? Score { get; set; } // Nullable!
            public DateTime Created { get; set; }
            public bool IsActive { get; set; }
        }

        [Fact]
        public void FromObjects_Creates_Populated_DataFrame()
        {
            var now = DateTime.Now;
            var users = new List<User>
            {
                new User { Id = 1, Name = "Alice", Score = 99.5, Created = now, IsActive = true },
                new User { Id = 2, Name = "Bob", Score = null, Created = now.AddDays(1), IsActive = false }
            };

            var df = DataFrame.FromObjects(users);

            // Verify Structure
            Assert.Equal(2, df.RowCount);
            Assert.Equal(5, df.ColumnCount);

            // Check Schema
            Assert.Equal(typeof(int), df["Id"].DataType);
            Assert.Equal(typeof(double), df["Score"].DataType);
            Assert.True(df["Score"].IsNullable); // Should detect int? as nullable

            // Check Data
            Assert.Equal(1, df["Id"].Get<int>(0));
            Assert.Equal("Bob", df["Name"].Get<string>(1));

            // Check Nullable
            Assert.Equal(99.5, df["Score"].Get<double>(0));
            Assert.True(df["Score"].IsNull(1));
        }

        [Fact]
        public void FromObjects_Skips_Unsupported_Types()
        {
            var list = new[] { new { Id = 1, Complex = new object() } }; // Complex object should be skipped
            var df = DataFrame.FromObjects(list);

            Assert.Equal(1, df.ColumnCount);
            Assert.True(df.Schema.HasColumn("Id"));
            Assert.False(df.Schema.HasColumn("Complex"));
        }
    }
}