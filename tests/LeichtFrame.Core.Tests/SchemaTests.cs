namespace LeichtFrame.Core.Tests;

public class SchemaTests
{
    [Fact]
    public void Can_Create_Schema_And_Lookup_Columns()
    {
        // Arrange
        var defs = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Name", typeof(string), IsNullable: true),
            new("Price", typeof(double))
        };

        // Act
        var schema = new DataFrameSchema(defs);

        // Assert
        Assert.Equal(3, schema.Columns.Count);

        Assert.True(schema.HasColumn("Id"));
        Assert.True(schema.HasColumn("Name"));
        Assert.False(schema.HasColumn("Address")); // Should not exist

        Assert.Equal(0, schema.GetColumnIndex("Id"));
        Assert.Equal(1, schema.GetColumnIndex("Name"));
    }

    [Fact]
    public void Duplicate_Column_Names_Should_Throw()
    {
        var defs = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Id", typeof(string)) // Duplicate
        };

        Assert.Throws<ArgumentException>(() => new DataFrameSchema(defs));
    }

    [Fact]
    public void Json_Serialization_Roundtrip_Works()
    {
        // Arrange
        var originalDefs = new List<ColumnDefinition>
        {
            new("Count", typeof(int)),
            new("IsActive", typeof(bool), IsNullable: true)
        };
        var originalSchema = new DataFrameSchema(originalDefs);

        // Act
        string json = originalSchema.ToJson();
        var loadedSchema = DataFrameSchema.FromJson(json);

        // Assert
        Assert.Equal(2, loadedSchema.Columns.Count);

        // Check first column
        Assert.Equal("Count", loadedSchema.Columns[0].Name);
        Assert.Equal(typeof(int), loadedSchema.Columns[0].DataType);
        Assert.False(loadedSchema.Columns[0].IsNullable);

        // Check second column
        Assert.Equal("IsActive", loadedSchema.Columns[1].Name);
        Assert.Equal(typeof(bool), loadedSchema.Columns[1].DataType);
        Assert.True(loadedSchema.Columns[1].IsNullable);
    }
}