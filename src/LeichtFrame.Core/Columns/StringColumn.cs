using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;
using LeichtFrame.Core.Engine.Memory;

namespace LeichtFrame.Core
{
    /// <summary>
    /// A high-performance column for storing variable-length strings using Arrow-style layout.
    /// Data is stored as contiguous UTF-8 bytes to minimize GC pressure and improve locality.
    /// </summary>
    public class StringColumn : Column<string?>, IDisposable
    {
        // 1. Offsets: Start position of each string in the byte buffer. Size: RowCount + 1
        private int[] _offsets;

        // 2. Values: The concatenated UTF-8 bytes of all strings.
        private byte[] _values;

        // 3. Nulls: Bitmap for null values.
        private NullBitmap? _nulls;
        internal NullBitmap? InternalNulls => _nulls;

        private int _length;          // Number of rows
        private int _totalByteCount;  // Currently used bytes in _values

        /// <summary>
        /// Initializes a new instance of the <see cref="StringColumn"/> class.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <param name="capacity">The initial row capacity.</param>
        /// <param name="isNullable">Whether nulls are allowed.</param>
        public StringColumn(string name, int capacity = 16, bool isNullable = false)
            : base(name, isNullable)
        {
            _length = 0;
            _totalByteCount = 0;

            // Offsets always need N+1 entries (Start of next is End of current)
            _offsets = ArrayPool<int>.Shared.Rent(capacity + 1);
            _offsets[0] = 0;

            // Heuristic: Assume average string length is 32 bytes.
            // This prevents frequent resizing at the beginning.
            int initialByteCapacity = capacity * 32;
            if (initialByteCapacity < 64) initialByteCapacity = 64;

            _values = ArrayPool<byte>.Shared.Rent(initialByteCapacity);

            if (isNullable)
            {
                _nulls = new NullBitmap(capacity);
            }
        }

        /// <inheritdoc />
        public override int Length => _length;

        /// <summary>
        /// Not supported for variable length layout directly as contiguous memory.
        /// Use <see cref="Get(int)"/> or specialized Span accessors.
        /// </summary>
        /// <exception cref="NotSupportedException">Always thrown.</exception>
        public override ReadOnlyMemory<string?> Values => throw new NotSupportedException(
            "StringColumn uses Arrow-style byte storage. Contiguous string references are not available.");

        // --- Core Access ---

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override string? Get(int index)
        {
            CheckBounds(index);

            if (_nulls != null && _nulls.IsNull(index))
                return null;

            int start = _offsets[index];
            int end = _offsets[index + 1]; // Length = Offset[i+1] - Offset[i]
            int byteLen = end - start;

            if (byteLen == 0) return string.Empty;

            // Allocates a new string (Lazy Decoding)
            return Encoding.UTF8.GetString(_values, start, byteLen);
        }

        /// <summary>
        /// Sets the value at the specified index.
        /// <para>
        /// ⚠️ **PERFORMANCE WARNING:** This operation is **O(N)** because it requires shifting all subsequent bytes 
        /// in the underlying buffer to accommodate the new string length. 
        /// Use this method only for corrections, not for bulk updates.
        /// </para>
        /// </summary>
        /// <param name="index">The zero-based row index.</param>
        /// <param name="value">The new string value.</param>
        public override void SetValue(int index, string? value)
        {
            CheckBounds(index);

            // 1. Determine old length
            int oldStart = _offsets[index];
            int oldLen = _offsets[index + 1] - oldStart;

            // 2. Calculate new length
            int newLen = 0;
            byte[]? newBytes = null;

            if (value != null)
            {
                // Optimization: Encoding.UTF8.GetByteCount would be faster for alloc check,
                // but we need the bytes anyway.
                newBytes = Encoding.UTF8.GetBytes(value);
                newLen = newBytes.Length;
            }

            // 3. Calculate shift difference
            int diff = newLen - oldLen;

            // Make space or close gap?
            if (diff > 0)
            {
                EnsureByteCapacity(_totalByteCount + diff);
                // Shift Right: Move everything after the old string to the right
                Array.Copy(_values, oldStart + oldLen,
                           _values, oldStart + newLen,
                           _totalByteCount - (oldStart + oldLen));
            }
            else if (diff < 0)
            {
                // Shift Left: Pull everything forward
                Array.Copy(_values, oldStart + oldLen,
                           _values, oldStart + newLen,
                           _totalByteCount - (oldStart + oldLen));
            }

            // 4. Write data
            if (newBytes != null)
            {
                Array.Copy(newBytes, 0, _values, oldStart, newLen);
                if (IsNullable) SetNotNull(index);
            }
            else
            {
                if (!IsNullable) throw new ArgumentNullException(nameof(value), "Column is not nullable");
                _nulls?.SetNull(index);
            }

            // 5. Update offsets (for ALL subsequent rows!)
            for (int i = index + 1; i <= _length; i++)
            {
                _offsets[i] += diff;
            }

            _totalByteCount += diff;
        }

        /// <inheritdoc />
        public override void Append(string? value)
        {
            EnsureCapacity(_length + 1);

            int byteLen = 0;
            if (value != null)
            {
                // 1. Calculate bytes
                byteLen = Encoding.UTF8.GetByteCount(value);

                // 2. Ensure byte capacity
                EnsureByteCapacity(_totalByteCount + byteLen);

                // 3. Write bytes
                Encoding.UTF8.GetBytes(value, 0, value.Length, _values, _totalByteCount);

                if (_nulls != null) _nulls.SetNotNull(_length);
            }
            else
            {
                if (_nulls == null)
                    throw new InvalidOperationException("Cannot append null to non-nullable column.");

                _nulls.SetNull(_length);
            }

            // 4. Update pointers
            _totalByteCount += byteLen;
            _offsets[_length + 1] = _totalByteCount;

            _length++;
        }

        /// <summary>
        /// Compares the string at indexA with the string at indexB directly on the byte buffer.
        /// Returns -1, 0, or 1.
        /// Zero-Allocation implementation.
        /// </summary>
        public int CompareRaw(int indexA, int indexB)
        {
            // 1. Null Handling
            bool nullA = IsNull(indexA);
            bool nullB = IsNull(indexB);

            if (nullA && nullB) return 0;
            if (nullA) return -1; // Null is smaller
            if (nullB) return 1;

            // 2. Get Spans (Zero-Copy pointers)
            int startA = _offsets[indexA];
            int lenA = _offsets[indexA + 1] - startA;
            ReadOnlySpan<byte> spanA = _values.AsSpan(startA, lenA);

            int startB = _offsets[indexB];
            int lenB = _offsets[indexB + 1] - startB;
            ReadOnlySpan<byte> spanB = _values.AsSpan(startB, lenB);

            // 3. Compare Bytes directly
            // SequenceCompareTo is an optimized .NET intrinsic method
            return spanA.SequenceCompareTo(spanB);
        }

        /// <summary>
        /// Computes the hash code of the string at the specified index directly from the byte buffer.
        /// Zero-Allocation.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCodeRaw(int index)
        {
            // 1. Null Check
            if (_nulls != null && _nulls.IsNull(index)) return 0;

            // 2. Locate Bytes
            int start = _offsets[index];
            int length = _offsets[index + 1] - start;

            if (length == 0) return string.Empty.GetHashCode();

            // 3. Compute Hash (FNV-1a or similar fast hash)            
            int hash = unchecked((int)2166136261);

            var span = _values.AsSpan(start, length);

            for (int i = 0; i < span.Length; i++)
            {
                hash ^= span[i];
                hash *= 16777619;
            }

            return hash;
        }

        // --- INTERNAL ACCESS FOR OPTIMIZED OPS ---

        /// <summary>Internal access to raw offsets for high-performance grouping/sorting.</summary>
        internal int[] Offsets => _offsets;

        /// <summary>Internal access to raw bytes for high-performance grouping/sorting.</summary>
        internal byte[] RawBytes => _values;

        // --- Null Handling ---

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override bool IsNull(int index)
        {
            CheckBounds(index);
            return _nulls != null && _nulls.IsNull(index);
        }

        /// <inheritdoc />
        public override void SetNull(int index)
        {
            // We use SetValue to correctly shift bytes (length 0).
            SetValue(index, null);
        }

        /// <inheritdoc />
        public override void SetNotNull(int index)
        {
            CheckBounds(index);
            _nulls?.SetNotNull(index);
        }

        // --- Memory ---

        /// <inheritdoc />
        public override void EnsureCapacity(int minCapacity)
        {
            // 1. Resize Offsets Array (Rows)
            if (_offsets.Length < minCapacity + 1)
            {
                int newCap = Math.Max(_offsets.Length * 2, minCapacity + 1);
                var newBuf = ArrayPool<int>.Shared.Rent(newCap);
                Array.Copy(_offsets, newBuf, _length + 1);
                ArrayPool<int>.Shared.Return(_offsets);
                _offsets = newBuf;

                _nulls?.Resize(newCap);
            }
        }

        internal void EnsureByteCapacity(int minBytes)
        {
            if (_values.Length < minBytes)
            {
                int newCap = Math.Max(_values.Length * 2, minBytes);
                var newBuf = ArrayPool<byte>.Shared.Rent(newCap);

                Array.Copy(_values, newBuf, _totalByteCount);

                ArrayPool<byte>.Shared.Return(_values);
                _values = newBuf;
            }
        }

        private void CheckBounds(int index)
        {
            if ((uint)index >= (uint)_length) throw new IndexOutOfRangeException();
        }

        /// <inheritdoc />
        public override IColumn CloneSubset(IReadOnlyList<int> indices)
        {
            int newRowCount = indices.Count;
            var newCol = new StringColumn(Name, newRowCount, IsNullable);

            int totalBytesNeeded = 0;

            if (indices is int[] arrIndices)
            {
                for (int i = 0; i < newRowCount; i++)
                {
                    int idx = arrIndices[i];
                    if (!IsNullable || !IsNull(idx))
                    {
                        totalBytesNeeded += _offsets[idx + 1] - _offsets[idx];
                    }
                }
            }
            else
            {
                for (int i = 0; i < newRowCount; i++)
                {
                    int idx = indices[i];
                    if (!IsNullable || !IsNull(idx))
                    {
                        totalBytesNeeded += _offsets[idx + 1] - _offsets[idx];
                    }
                }
            }

            newCol.EnsureByteCapacity(totalBytesNeeded);

            int currentDestOffset = 0;

            var destOffsets = newCol._offsets;
            var destBytes = newCol._values;
            var destNulls = newCol._nulls;

            destOffsets[0] = 0;

            for (int i = 0; i < newRowCount; i++)
            {
                int srcIdx = indices[i];

                if (IsNullable && IsNull(srcIdx))
                {
                    destNulls!.SetNull(i);
                    destOffsets[i + 1] = currentDestOffset;
                    continue;
                }

                int start = _offsets[srcIdx];
                int length = _offsets[srcIdx + 1] - start;

                if (length > 0)
                {
                    Array.Copy(_values, start, destBytes, currentDestOffset, length);
                    currentDestOffset += length;
                }

                destOffsets[i + 1] = currentDestOffset;
            }

            newCol._length = newRowCount;
            newCol._totalByteCount = currentDestOffset;

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