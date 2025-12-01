namespace LeichtFrame.Core.Tests.DataFrames.Operations
{
    public class SelectionTests
    {
        [Fact]
        public void Select_Returns_Subset_Of_Columns()
        {
            // Arrange
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("A", typeof(int)),
                new ColumnDefinition("B", typeof(int)),
                new ColumnDefinition("C", typeof(int))
            });
            var df = DataFrame.Create(schema, 10);

            // Act
            var selected = df.Select("A", "C");

            // Assert
            Assert.Equal(2, selected.ColumnCount);
            Assert.Equal("A", selected.Columns[0].Name);
            Assert.Equal("C", selected.Columns[1].Name);

            // Verify B is gone
            Assert.False(selected.TryGetColumn("B", out _));
        }

        [Fact]
        public void Select_Is_ZeroCopy_And_SharedReference()
        {
            // Arrange
            var df = DataFrame.Create(new DataFrameSchema(new[] { new ColumnDefinition("Val", typeof(int)) }), 5);
            var colOriginal = (IntColumn)df["Val"];
            colOriginal.Append(100);

            // Act
            var dfSelection = df.Select("Val");
            var colSelected = (IntColumn)dfSelection["Val"];

            // Assert 1: They are physically the same objects
            Assert.Same(colOriginal, colSelected);

            // Assert 2: Changes in the original are visible in the selection
            colOriginal.SetValue(0, 999);
            Assert.Equal(999, colSelected.Get(0));

            // Assert 3: Changes in the selection are visible in the original
            colSelected.SetValue(0, 555);
            Assert.Equal(555, colOriginal.Get(0));
        }

        [Fact]
        public void Select_Respects_Order()
        {
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("A", typeof(int)),
                new ColumnDefinition("B", typeof(int))
            });
            var df = DataFrame.Create(schema, 10);

            // Select B first, then A
            var selected = df.Select("B", "A");

            Assert.Equal("B", selected.Columns[0].Name);
            Assert.Equal("A", selected.Columns[1].Name);
        }

        [Fact]
        public void Select_Throws_On_Missing_Column()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] { new ColumnDefinition("A", typeof(int)) }), 5);

            Assert.Throws<ArgumentException>(() => df.Select("Z"));
        }
    }
}