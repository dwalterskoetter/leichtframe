using System.Buffers;
using System.Text;

namespace LeichtFrame.Core
{
    /// <summary>
    /// A high-performance column for variable-length string data using Arrow-style layout.
    /// Stores data as a contiguous UTF-8 byte buffer and an offset array.
    /// </summary>
    public class VariableLengthColumn : Column<string?>, IDisposable
    {
        // 1. The offsets array (N+1 entries). 
        // _offsets[i] is the start index in _values.
        // _offsets[i+1] - _offsets[i] is the length in bytes.
        private int[] _offsets;

        // 2. The contiguous data buffer (UTF-8 bytes).
        private byte[] _values;

        // 3. Null handling
        private NullBitmap? _nulls;

        private int _rowCount;
        private int _totalByteCount;

        /// <summary>
        /// Initializes a new instance of the <see cref="VariableLengthColumn"/> class.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <param name="capacity">The initial capacity (number of rows) to allocate.</param>
        /// <param name="isNullable">Whether the column supports null values.</param>
        public VariableLengthColumn(string name, int capacity = 16, bool isNullable = false)
            : base(name, isNullable)
        {
            _rowCount = 0;
            _totalByteCount = 0;

            // Offsets need rowCount + 1
            _offsets = ArrayPool<int>.Shared.Rent(capacity + 1);
            _offsets[0] = 0; // Always start at 0

            // Estimate 32 bytes per string initially (can grow)
            _values = ArrayPool<byte>.Shared.Rent(capacity * 32);

            if (isNullable)
            {
                _nulls = new NullBitmap(capacity);
            }
        }

        /// <inheritdoc />
        public override int Length => _rowCount;

        /// <summary>
        /// Gets the raw values as memory.
        /// <para>
        /// Not supported for <see cref="VariableLengthColumn"/> efficiently as it would require materializing all strings.
        /// Use <see cref="Get(int)"/> for access.
        /// </para>
        /// </summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public override ReadOnlyMemory<string?> Values => throw new NotSupportedException("Use specialized accessors or Get()");

        // --- Core Access ---

        /// <inheritdoc />
        public override string? Get(int index)
        {
            CheckBounds(index);

            if (_nulls != null && _nulls.IsNull(index))
                return null;

            int start = _offsets[index];
            int end = _offsets[index + 1];
            int length = end - start;

            // Zero-Allocation lookup if length is 0
            if (length == 0) return string.Empty;

            return Encoding.UTF8.GetString(_values, start, length);
        }

        /// <summary>
        /// Random write access is not supported for VariableLengthColumn as it requires shifting bytes in the buffer.
        /// Use <see cref="Append(string?)"/> instead.
        /// </summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public override void SetValue(int index, string? value)
        {
            // Setting values in a packed column is expensive because we have to shift bytes!
            // For MVP, we throw. This column is optimized for Append-only or Bulk-load.
            throw new NotSupportedException("Random write (SetValue) is not supported on VariableLengthColumn due to byte shifting. Use Append.");
        }

        // --- Appending (The Hot Path) ---

        /// <inheritdoc />
        public override void Append(string? value)
        {
            EnsureRowCapacity(_rowCount + 1);

            if (value == null)
            {
                if (_nulls == null)
                    throw new InvalidOperationException("Cannot append null to non-nullable column.");

                _nulls.SetNull(_rowCount);

                // Null string has 0 length, so next offset = current offset
                _offsets[_rowCount + 1] = _offsets[_rowCount];
            }
            else
            {
                // 1. Calculate bytes needed
                int byteCount = Encoding.UTF8.GetByteCount(value);

                // 2. Ensure byte buffer capacity
                EnsureByteCapacity(_totalByteCount + byteCount);

                // 3. Write bytes directly to buffer
                Encoding.UTF8.GetBytes(value, 0, value.Length, _values, _totalByteCount);

                // 4. Update counters
                _totalByteCount += byteCount;
                _offsets[_rowCount + 1] = _totalByteCount;

                if (_nulls != null) _nulls.SetNotNull(_rowCount);
            }

            _rowCount++;
        }

        // --- Memory Management ---

        /// <inheritdoc />
        public override void EnsureCapacity(int minRows)
        {
            EnsureRowCapacity(minRows);
        }

        private void EnsureRowCapacity(int minRows)
        {
            // Offsets array must hold (minRows + 1) items
            if (_offsets.Length >= minRows + 1) return;

            int newCapacity = Math.Max(_offsets.Length * 2, minRows + 1);
            var newOffsets = ArrayPool<int>.Shared.Rent(newCapacity);

            Array.Copy(_offsets, newOffsets, _rowCount + 1);
            ArrayPool<int>.Shared.Return(_offsets);
            _offsets = newOffsets;

            _nulls?.Resize(minRows);
        }

        private void EnsureByteCapacity(int minBytes)
        {
            if (_values.Length >= minBytes) return;

            int newCapacity = Math.Max(_values.Length * 2, minBytes);
            var newValues = ArrayPool<byte>.Shared.Rent(newCapacity);

            Array.Copy(_values, newValues, _totalByteCount);
            ArrayPool<byte>.Shared.Return(_values);
            _values = newValues;
        }

        private void CheckBounds(int index)
        {
            if ((uint)index >= (uint)_rowCount) throw new IndexOutOfRangeException();
        }

        // --- Interface Impl ---

        /// <inheritdoc />
        public override bool IsNull(int index)
        {
            CheckBounds(index);
            return _nulls != null && _nulls.IsNull(index);
        }

        /// <summary>
        /// Not supported for VariableLengthColumn.
        /// </summary>
        public override void SetNull(int index)
        {
            throw new NotSupportedException("Random access SetNull not supported on VariableLengthColumn.");
        }

        /// <summary>
        /// Not supported for VariableLengthColumn.
        /// </summary>
        public override void SetNotNull(int index)
        {
            throw new NotSupportedException("Random access SetNotNull not supported.");
        }

        /// <inheritdoc />
        public override IColumn CloneSubset(IReadOnlyList<int> indices)
        {
            var newCol = new VariableLengthColumn(Name, indices.Count, IsNullable);
            foreach (var idx in indices)
            {
                newCol.Append(Get(idx));
            }
            return newCol;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (_offsets != null)
            {
                ArrayPool<int>.Shared.Return(_offsets);
                _offsets = null!;
            }
            if (_values != null)
            {
                ArrayPool<byte>.Shared.Return(_values);
                _values = null!;
            }
            _nulls?.Dispose();
            _nulls = null;
        }
    }
}