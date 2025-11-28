namespace LeichtFrame.Core;

/// Typed base class for columns.
/// The type of data stored (int, double, string, etc.)
public abstract class Column<T> : Column, IColumn<T>
{
    protected Column(string name, bool isNullable = false) : base(name, typeof(T), isNullable)
    {
    }

    /// Access to the underlying memory storage.
    public abstract ReadOnlyMemory<T> Values { get; }

    /// Gets the value at the specified index.
    public abstract T GetValue(int index);

    /// Sets the value at the specified index.
    public abstract void SetValue(int index, T value);

    // --- Null Handling API ---
    /// Checks if the value at the index is logically null.
    public abstract bool IsNull(int index);

    /// Marks the value at the index as null.
    public abstract void SetNull(int index);

    /// Marks the value at the index as valid (not null).
    public abstract void SetNotNull(int index);

    public virtual ReadOnlyMemory<T> Slice(int start, int length)
    {
        // 1. Bounds Safety
        if ((uint)start > (uint)Length || (uint)length > (uint)(Length - start))
        {
            throw new ArgumentOutOfRangeException(nameof(start),
                $"Slice range {start}..{start + length} is out of bounds (Length: {Length}).");
        }

        // 2. Zero-Copy Delegation
        // Calls the 'Values' property of the concrete class (IntColumn, DoubleColumn, etc.)
        // and uses .NET built-in slicing for Memory<T>.
        return Values.Slice(start, length);
    }

    /// Implementation for IColumn<T>.AsSpan
    public virtual ReadOnlySpan<T> AsSpan() => Values.Span;
}