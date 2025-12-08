using System;
using System.Collections.Generic;

namespace LeichtFrame.Core
{
    /// <summary>
    /// A zero-copy view over a subset of another column.
    /// Delegates calls to the source column with an index offset without allocating new memory for data.
    /// </summary>
    /// <typeparam name="T">The type of data stored in the column.</typeparam>
    public class SlicedColumn<T> : IColumn<T>, IDisposable
    {
        private readonly IColumn<T> _source;
        private readonly int _offset;
        private readonly int _length;

        /// <summary>
        /// Initializes a new instance of the <see cref="SlicedColumn{T}"/> class.
        /// </summary>
        /// <param name="source">The underlying source column.</param>
        /// <param name="offset">The zero-based starting index in the source column.</param>
        /// <param name="length">The number of rows in the slice.</param>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if offset or length are negative.</exception>
        /// <exception cref="ArgumentException">Thrown if the slice range exceeds the source column bounds.</exception>
        public SlicedColumn(IColumn<T> source, int offset, int length)
        {
            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset));
            if (length < 0) throw new ArgumentOutOfRangeException(nameof(length));

            if (offset + length > source.Length)
                throw new ArgumentException($"Slice range ({offset}..{offset + length}) exceeds source column bounds (Length: {source.Length}).");

            _source = source;
            _offset = offset;
            _length = length;
        }

        /// <inheritdoc />
        public string Name => _source.Name;

        /// <inheritdoc />
        public Type DataType => _source.DataType;

        /// <inheritdoc />
        public int Length => _length;

        /// <inheritdoc />
        public bool IsNullable => _source.IsNullable;

        /// <inheritdoc />
        public ReadOnlyMemory<T> Values => _source.Slice(_offset, _length);

        /// <inheritdoc />
        public ReadOnlySpan<T> AsSpan() => Values.Span;

        // --- Data Access ---

        /// <inheritdoc />
        public T Get(int index)
        {
            CheckBounds(index);
            return _source.GetValue(index + _offset);
        }

        // Interface Implementation
        T IColumn<T>.GetValue(int index) => Get(index);

        /// <inheritdoc />
        public object? GetValue(int index)
        {
            CheckBounds(index);
            return _source.GetValue(index + _offset);
        }

        /// <inheritdoc />
        public void SetValue(int index, T value)
        {
            CheckBounds(index);
            _source.SetValue(index + _offset, value);
        }

        // --- Appending (Not Supported for Views) ---
        // Views cannot grow, so we explicitly forbid appending.

        /// <summary>
        /// Not supported for SlicedColumn. Slices have a fixed size.
        /// </summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public void Append(T value)
        {
            throw new NotSupportedException("Cannot append to a SlicedColumn view. Append to the source column instead.");
        }

        /// <summary>
        /// Not supported for SlicedColumn. Slices have a fixed size.
        /// </summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public void AppendObject(object? value)
        {
            throw new NotSupportedException("Cannot append to a SlicedColumn view. Append to the source column instead.");
        }

        /// <summary>
        /// Not supported for SlicedColumn.
        /// </summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public void EnsureCapacity(int capacity)
        {
            throw new NotSupportedException("Cannot resize a SlicedColumn view.");
        }

        // --- Slicing ---

        /// <inheritdoc />
        public ReadOnlyMemory<T> Slice(int start, int length)
        {
            CheckBounds(start);
            if (start + length > _length) throw new ArgumentOutOfRangeException(nameof(length));

            // Delegate to source slice with accumulated offset
            return _source.Slice(start + _offset, length);
        }

        /// <inheritdoc />
        public IColumn CloneSubset(IReadOnlyList<int> indices)
        {
            var mappedIndices = new int[indices.Count];
            for (int i = 0; i < indices.Count; i++)
            {
                if (indices[i] < 0 || indices[i] >= _length)
                    throw new IndexOutOfRangeException();

                mappedIndices[i] = indices[i] + _offset;
            }
            return _source.CloneSubset(mappedIndices);
        }

        // --- Null Handling ---

        /// <inheritdoc />
        public bool IsNull(int index)
        {
            CheckBounds(index);
            return _source.IsNull(index + _offset);
        }

        /// <inheritdoc />
        public void SetNull(int index)
        {
            CheckBounds(index);
            _source.SetNull(index + _offset);
        }

        // --- Helpers ---

        /// <inheritdoc />
        public void Dispose()
        {
            // Do nothing. We do NOT own the underlying memory.
        }

        private void CheckBounds(int index)
        {
            if ((uint)index >= (uint)_length)
                throw new IndexOutOfRangeException($"Index {index} is out of slice bounds (Length {_length})");
        }
    }
}