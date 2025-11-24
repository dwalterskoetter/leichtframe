using LeichtFrame.Core;


public class ColumnFactoryTests
{
    [Fact]
    public void Create_WithIntType_ReturnsIntColumn()
    {
        var col = ColumnFactory.Create("age", typeof(int), capacity: 32);
        Assert.NotNull(col);
        Assert.IsType<IntColumn>(col);
        Assert.Equal("age", col.Name);
        Assert.Equal(typeof(int), col.DataType);
    }

    [Fact]
    public void Create_GenericInt_Returns_IColumnOfInt()
    {
        var col = ColumnFactory.Create<int>("age", capacity: 16);
        Assert.NotNull(col);
        Assert.IsAssignableFrom<IColumn<int>>(col);
        Assert.Equal("age", col.Name);
        Assert.Equal(typeof(int), col.DataType);
    }

    [Fact]
    public void Create_UnsupportedType_Throws()
    {
        Assert.Throws<NotSupportedException>(() =>
        {
            ColumnFactory.Create("obj", typeof(DateTimeOffset), capacity: 4);
        });
    }
}
