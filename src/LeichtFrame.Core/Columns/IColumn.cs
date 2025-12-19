namespace LeichtFrame.Core
{
    /// <summary>
    /// Represents a generic column in a DataFrame containing metadata and operations.
    /// </summary>
    public interface IColumn
    {
        /// <summary>
        /// Gets the unique name of the column.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Gets the CLR type of the data stored in this column.
        /// </summary>
        Type DataType { get; }

        /// <summary>
        /// Gets the number of rows in this column.
        /// </summary>
        int Length { get; }

        /// <summary>
        /// Indicates whether the column supports null values.
        /// </summary>
        bool IsNullable { get; }

        /// <summary>
        /// Ensures the column has space for at least the specified number of elements.
        /// If the capacity is increased, the underlying buffer is swapped.           
        /// <para>
        /// **SAFETY WARNING:** Because this library uses array pooling, 
        /// calling this method may return the old buffer to the pool.  
        /// Existing Spans pointing to the old buffer will become invalid.
        /// </para>
        /// </summary>
        /// <param name="capacity">The minimum required capacity.</param>
        void EnsureCapacity(int capacity);

        /// <summary>
        /// Gets the value at the specified index as an object (boxed).
        /// For high performance, use the typed interface <see cref="IColumn{T}"/>.
        /// </summary>
        /// <param name="index">The zero-based row index.</param>
        /// <returns>The value at the index, or null.</returns>
        object? GetValue(int index);

        /// <summary>
        /// Checks if the value at the specified index is null.
        /// </summary>
        bool IsNull(int index);

        /// <summary>
        /// Sets the value at the specified index to null.
        /// </summary>
        void SetNull(int index);

        /// <summary>
        /// Appends an untyped value to the end of the column.
        /// </summary>
        void AppendObject(object? value);

        /// <summary>
        /// Creates a deep copy of the column containing only the rows at the specified indices.
        /// </summary>
        /// <param name="indices">The list of row indices to copy.</param>
        /// <returns>A new column containing the subset of data.</returns>
        IColumn CloneSubset(IReadOnlyList<int> indices);

        /// <summary>
        /// Computes the sum for a subset of rows defined by indices.
        /// </summary>
        /// <param name="indices">The global index array.</param>
        /// <param name="start">Start offset in the index array.</param>
        /// <param name="end">End offset in the index array.</param>
        object? ComputeSum(int[] indices, int start, int end);

        /// <summary>
        /// Computes the mean for a subset of rows defined by indices.
        /// </summary>
        object? ComputeMean(int[] indices, int start, int end);

        /// <summary>
        /// Finds the minimum value for a subset of rows defined by indices.
        /// </summary>
        object? ComputeMin(int[] indices, int start, int end);

        /// <summary>
        /// Finds the maximum value for a subset of rows defined by indices.
        /// </summary>
        object? ComputeMax(int[] indices, int start, int end);
    }

    /// <summary>
    /// Typed interface for high-performance, zero-boxing data access.
    /// </summary>
    /// <typeparam name="T">The type of data stored in the column.</typeparam>
    public interface IColumn<T> : IColumn
    {
        /// <summary>
        /// Gets the strongly-typed value at the specified index.
        /// </summary>
        new T GetValue(int index);

        /// <summary>
        /// Sets the strongly-typed value at the specified index.
        /// </summary>
        void SetValue(int index, T value);


        /// <summary>
        /// Appends a strongly-typed value to the end of the column.
        /// </summary>
        void Append(T value);

        /// <summary>
        /// Returns a zero-copy view of the column data as a Memory region.
        /// </summary>
        ReadOnlyMemory<T> Slice(int start, int length);

        /// <summary>
        /// Returns the underlying data as a ReadOnlySpan for high-performance processing.
        /// </summary>
        ReadOnlySpan<T> AsSpan();
    }
}