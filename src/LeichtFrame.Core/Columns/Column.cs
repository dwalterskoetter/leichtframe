namespace LeichtFrame.Core;

/// <summary>
/// Non-generic base class for all columns. 
/// Allows storing columns of different types in a single collection.
/// </summary>
public abstract class Column : IColumn
{
    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public Type DataType { get; }

    /// <inheritdoc />
    public bool IsNullable { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="Column"/> class.
    /// </summary>
    /// <param name="name">The unique name of the column.</param>
    /// <param name="dataType">The CLR type of the data stored.</param>
    /// <param name="isNullable">Whether the column allows null values.</param>
    /// <exception cref="ArgumentException">Thrown if name is null or whitespace.</exception>
    /// <exception cref="ArgumentNullException">Thrown if dataType is null.</exception>
    protected Column(string name, Type dataType, bool isNullable)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Column name cannot be null or empty.", nameof(name));

        Name = name;
        DataType = dataType ?? throw new ArgumentNullException(nameof(dataType));
        IsNullable = isNullable;
    }

    /// <inheritdoc />
    public abstract int Length { get; }

    /// <inheritdoc />
    public abstract void EnsureCapacity(int capacity);

    /// <inheritdoc />
    public abstract object? GetValue(int index);

    /// <inheritdoc />
    public abstract void AppendObject(object? value);

    /// <inheritdoc />
    public abstract IColumn CloneSubset(IReadOnlyList<int> indices);

    /// <inheritdoc />
    public abstract bool IsNull(int index);

    /// <inheritdoc />
    public abstract void SetNull(int index);
}