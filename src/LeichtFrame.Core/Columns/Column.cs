namespace LeichtFrame.Core;

/// <summary>
/// Non-generic base class for all columns. 
/// Allows storing columns of different types in a single collection.
/// </summary>
public abstract class Column : IColumn
{
    public string Name { get; }
    public Type DataType { get; }
    public bool IsNullable { get; }

    protected Column(string name, Type dataType, bool isNullable)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Column name cannot be null or empty.", nameof(name));

        Name = name;
        DataType = dataType ?? throw new ArgumentNullException(nameof(dataType));
        IsNullable = isNullable;
    }

    public abstract int Length { get; }

    public abstract void EnsureCapacity(int capacity);

    public abstract object? GetValue(int index);

    public abstract void AppendObject(object? value);

    public abstract IColumn CloneSubset(IReadOnlyList<int> indices);

    public abstract bool IsNull(int index);

    public abstract void SetNull(int index);
}