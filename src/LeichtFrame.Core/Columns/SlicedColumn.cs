using System;
using System.Collections.Generic;

namespace LeichtFrame.Core
{
    /// <summary>
    /// A zero-copy view over a subset of another column.
    /// Delegates calls to the source column with an index offset.
    /// </summary>
    public class SlicedColumn<T> : IColumn<T>, IDisposable
    {
        private readonly IColumn<T> _source;
        private readonly int _offset;
        private readonly int _length;

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

        public string Name => _source.Name;
        public Type DataType => _source.DataType;
        public int Length => _length;
        public bool IsNullable => _source.IsNullable;

        public ReadOnlyMemory<T> Values => _source.Slice(_offset, _length);

        public ReadOnlySpan<T> AsSpan() => Values.Span;

        // --- Data Access ---

        public T Get(int index)
        {
            CheckBounds(index);
            return _source.GetValue(index + _offset);
        }

        // Interface Implementation
        T IColumn<T>.GetValue(int index) => Get(index);

        public object? GetValue(int index)
        {
            CheckBounds(index);
            return _source.GetValue(index + _offset);
        }

        public void SetValue(int index, T value)
        {
            CheckBounds(index);
            _source.SetValue(index + _offset, value);
        }

        // --- Appending (Not Supported for Views) ---
        // Views cannot grow, so we explicitly forbid appending.

        public void Append(T value)
        {
            throw new NotSupportedException("Cannot append to a SlicedColumn view. Append to the source column instead.");
        }

        public void AppendObject(object? value)
        {
            throw new NotSupportedException("Cannot append to a SlicedColumn view. Append to the source column instead.");
        }

        public void EnsureCapacity(int capacity)
        {
            throw new NotSupportedException("Cannot resize a SlicedColumn view.");
        }

        // --- Slicing ---

        public ReadOnlyMemory<T> Slice(int start, int length)
        {
            CheckBounds(start);
            if (start + length > _length) throw new ArgumentOutOfRangeException(nameof(length));

            // Delegate to source slice with accumulated offset
            return _source.Slice(start + _offset, length);
        }

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

        public bool IsNull(int index)
        {
            CheckBounds(index);
            return _source.IsNull(index + _offset);
        }

        public void SetNull(int index)
        {
            CheckBounds(index);
            _source.SetNull(index + _offset);
        }

        // --- Helpers ---

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