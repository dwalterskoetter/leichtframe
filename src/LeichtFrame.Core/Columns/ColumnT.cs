using System.Globalization;

namespace LeichtFrame.Core;

/// <summary>
/// Typed base class for columns storing specific data types (int, double, string, etc.).
/// </summary>
/// <typeparam name="T">The type of data stored in this column.</typeparam>
public abstract class Column<T> : Column, IColumn<T>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="Column{T}"/> class.
    /// </summary>
    /// <param name="name">The name of the column.</param>
    /// <param name="isNullable">Whether the column supports null values.</param>
    protected Column(string name, bool isNullable = false) : base(name, typeof(T), isNullable)
    {
    }

    /// <summary>
    /// Gets the underlying memory storage of the column.
    /// </summary>
    public abstract ReadOnlyMemory<T> Values { get; }

    /// <summary>
    /// Gets the strongly-typed value at the specified index.
    /// </summary>
    /// <param name="index">The zero-based row index.</param>
    /// <returns>The value of type T.</returns>
    public abstract T Get(int index);

    /// <inheritdoc />
    public abstract void SetValue(int index, T value);

    // --- Interface Implementations ---

    T IColumn<T>.GetValue(int index) => Get(index);

    /// <inheritdoc />
    public override object? GetValue(int index)
    {
        if (IsNullable && IsNull(index)) return null;
        return Get(index);
    }

    // --- Appending ---

    /// <inheritdoc />
    public abstract void Append(T value);

    // WICHTIG: Hier muss 'override' stehen, da es in 'Column' abstract ist.
    /// <inheritdoc />
    public override void AppendObject(object? value)
    {
        if (value is T typedVal)
        {
            Append(typedVal);
        }
        else if (value is null)
        {
            if (!IsNullable)
                throw new ArgumentException($"Cannot append null to non-nullable column '{Name}'.");

            Append(default!);
            SetNull(Length - 1);
        }
        else
        {
            try
            {
                var converted = (T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture);
                Append(converted);
            }
            catch
            {
                throw new ArgumentException($"Cannot convert '{value}' to {typeof(T).Name}");
            }
        }
    }

    // --- Null Handling ---

    /// <inheritdoc />
    public abstract override bool IsNull(int index);

    /// <inheritdoc />
    public abstract override void SetNull(int index);

    /// <summary>
    /// Marks the value at the specified index as not null.
    /// </summary>
    /// <param name="index">The zero-based row index.</param>
    public abstract void SetNotNull(int index);

    // --- Slicing ---

    /// <inheritdoc />
    public virtual ReadOnlyMemory<T> Slice(int start, int length)
    {
        if ((uint)start > (uint)Length || (uint)length > (uint)(Length - start))
        {
            throw new ArgumentOutOfRangeException(nameof(start),
                $"Slice range {start}..{start + length} is out of bounds (Length: {Length}).");
        }

        return Values.Slice(start, length);
    }

    /// <inheritdoc />
    public virtual ReadOnlySpan<T> AsSpan() => Values.Span;
}