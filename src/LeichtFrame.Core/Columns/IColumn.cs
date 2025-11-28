namespace LeichtFrame.Core
{
    /// Non-generic interface for column metadata and management.
    public interface IColumn
    {
        string Name { get; }
        Type DataType { get; }
        int Length { get; }
        bool IsNullable { get; }
        void EnsureCapacity(int capacity);
    }

    /// Typed interface for high-performance data access.
    public interface IColumn<T> : IColumn
    {
        T GetValue(int index);
        void SetValue(int index, T value);
        ReadOnlyMemory<T> Slice(int start, int length);

        // Useful for zero-copy access later
        ReadOnlySpan<T> AsSpan();
    }
}