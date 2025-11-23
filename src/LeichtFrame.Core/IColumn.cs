using System;

namespace LeichtFrame.Core
{
    /// Non-generic interface for column metadata and management.
    public interface IColumn
    {
        string Name { get; }
        Type DataType { get; }
        int Length { get; }

        /// Ensures the column has space for at least the specified number of elements.
        void EnsureCapacity(int capacity);
    }

    /// Typed interface for high-performance data access.
    public interface IColumn<T> : IColumn
    {
        T GetValue(int index);
        void SetValue(int index, T value);

        // Useful for zero-copy access later
        ReadOnlySpan<T> AsSpan();
    }
}