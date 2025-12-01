using System;

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

            // Allow 0 length slices even at the end of column
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

        // --- Memory / Span Access ---

        public ReadOnlyMemory<T> Values => _source.Slice(_offset, _length);

        public ReadOnlySpan<T> AsSpan() => Values.Span;

        // --- Core Access with Offset ---

        // 1. The method we want to use (Get)
        public T Get(int index)
        {
            CheckBounds(index);
            // We need to cast or go through the interface, 
            // because IColumn<T> in the interface is called "GetValue", but we know we want "Get".
            // The cleanest way, if _source also has "Get":
            // Unfortunately, "Get" is not in the IColumn<T> interface, but "GetValue" is.
            // So we call the interface:
            return _source.GetValue(index + _offset);
        }

        // 2. Explicit implementation for the interface
        T IColumn<T>.GetValue(int index) => Get(index);

        // 3. Untyped access (IColumn)
        public object? GetValue(int index)
        {
            CheckBounds(index);
            // Here we call the untyped method of the source
            return _source.GetValue(index + _offset);
        }

        public void SetValue(int index, T value)
        {
            CheckBounds(index);
            _source.SetValue(index + _offset, value);
        }

        public ReadOnlyMemory<T> Slice(int start, int length)
        {
            CheckBounds(start);
            if (start + length > _length) throw new ArgumentOutOfRangeException();

            // Delegate to source slice with accumulated offset
            return _source.Slice(start + _offset, length);
        }

        // --- No-Ops for View ---

        public void EnsureCapacity(int capacity)
        {
            throw new NotSupportedException("Cannot resize a SlicedColumn view.");
        }

        public void Dispose()
        {
            // Do nothing. We do NOT own the underlying memory.
        }

        private void CheckBounds(int index)
        {
            if ((uint)index >= (uint)_length)
                throw new IndexOutOfRangeException($"Index {index} is out of slice bounds (Length {_length})");
        }

        public IColumn CloneSubset(IReadOnlyList<int> indices)
        {
            // We have to map the indices to the original column (sourceIndex + _offset)
            var mappedIndices = new int[indices.Count];
            for (int i = 0; i < indices.Count; i++)
            {
                // Bounds check relative to the slice
                if (indices[i] < 0 || indices[i] >= _length)
                    throw new IndexOutOfRangeException();

                mappedIndices[i] = indices[i] + _offset;
            }

            // Delegate to the original column
            return _source.CloneSubset(mappedIndices);
        }

        public bool IsNull(int index)
        {
            CheckBounds(index);
            return _source.IsNull(index + _offset);
        }

        public void SetNull(int index)
        {
            CheckBounds(index);
            // Write-through to original
            _source.SetNull(index + _offset);
        }
    }
}