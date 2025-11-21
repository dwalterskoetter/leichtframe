using Xunit;
using LeichtFrame.Core.Tests.Mocks;
using System;

namespace LeichtFrame.Core.Tests;

public class ColumnTests
{
    [Fact]
    public void Column_Should_Have_Correct_Metadata()
    {
        // Arrange
        var col = new SimpleMockColumn<int>("Age", 10);

        // Assert
        Assert.Equal("Age", col.Name);
        Assert.Equal(typeof(int), col.DataType);
        Assert.Equal(10, col.Length);
    }

    [Fact]
    public void Column_Should_Throw_On_Invalid_Name()
    {
        Assert.Throws<ArgumentException>(() => new SimpleMockColumn<int>("", 10));
        Assert.Throws<ArgumentException>(() => new SimpleMockColumn<int>(null!, 10));
    }

    [Fact]
    public void Column_Get_Set_Values_Work()
    {
        // Arrange
        var col = new SimpleMockColumn<int>("Id", 5);

        // Act
        col.SetValue(0, 42);
        col.SetValue(2, 100);

        // Assert
        Assert.Equal(42, col.GetValue(0));
        Assert.Equal(0, col.GetValue(1));
        Assert.Equal(100, col.GetValue(2));
    }

    [Fact]
    public void Column_Null_Handling_Works()
    {
        // Arrange
        var col = new SimpleMockColumn<string>("Names", 3);

        // Act
        col.SetValue(0, "Alice");
        col.SetNull(1);

        // Assert
        Assert.False(col.IsNull(0));
        Assert.True(col.IsNull(1));
    }
}