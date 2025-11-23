using System;

namespace LeichtFrame.Core;

/// Typed base class for columns.
/// The type of data stored (int, double, string, etc.)
public abstract class Column<T> : Column
{
    protected Column(string name) : base(name, typeof(T))
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

    /// Implementation for IColumn<T>.AsSpan
    public virtual ReadOnlySpan<T> AsSpan() => Values.Span;
}