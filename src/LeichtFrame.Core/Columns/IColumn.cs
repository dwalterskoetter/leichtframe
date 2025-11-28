namespace LeichtFrame.Core
{
    /// Non-generic interface for column metadata and management.
    public interface IColumn
    {
        string Name { get; }
        Type DataType { get; }
        int Length { get; }
        bool IsNullable { get; }

        /// <summary>
        /// Ensures the column has space for at least the specified number of elements.
        /// If the capacity is increased, the underlying buffer is swapped.
        /// <para>
        /// <strong>SAFETY WARNING:</strong> Because this library uses array pooling, 
        /// calling this method (or appending data that triggers it) may return the old buffer to the pool.
        /// Any existing <see cref="ReadOnlySpan{T}"/> or <see cref="ReadOnlyMemory{T}"/> pointing to 
        /// the old buffer should be considered invalid/unsafe immediately after this call.
        /// </para>
        /// </summary>
        void EnsureCapacity(int capacity);
        object? GetValue(int index);
    }

    /// Typed interface for high-performance data access.
    public interface IColumn<T> : IColumn
    {
        new T GetValue(int index);
        void SetValue(int index, T value);
        ReadOnlyMemory<T> Slice(int start, int length);

        // Useful for zero-copy access later
        ReadOnlySpan<T> AsSpan();
    }
}