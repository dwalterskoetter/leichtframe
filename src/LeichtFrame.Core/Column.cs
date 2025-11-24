using System;

namespace LeichtFrame.Core;

/// The non-generic base class for all columns. 
/// Allows storing columns of different types in a single collection.
public abstract class Column : IColumn
{
    // Properties
    public string Name { get; }
    public Type DataType { get; }

    // Constructor
    protected Column(string name, Type dataType)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Column name cannot be null or empty.", nameof(name));

        Name = name;
        DataType = dataType ?? throw new ArgumentNullException(nameof(dataType));
    }

    /// Number of rows in this column.
    public abstract int Length { get; }

    // We will add non-generic GetValue methods here later if needed (object GetValue(int index))
    public abstract void EnsureCapacity(int capacity);
}