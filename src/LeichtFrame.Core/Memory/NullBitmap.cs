using System.Buffers;
using System.Runtime.CompilerServices;

namespace LeichtFrame.Core
{
    /// <summary>
    /// A memory-efficient bitset used to track null values in nullable columns.
    /// Uses 1 bit per row, resulting in very low memory overhead (approx. 1.5% of an integer array).
    /// </summary>
    public class NullBitmap : IDisposable
    {
        private ulong[] _buffer;
        private int _capacity;

        /// <summary>
        /// Initializes a new instance of the <see cref="NullBitmap"/> class.
        /// </summary>
        /// <param name="capacity">The initial number of bits (rows) to support.</param>
        public NullBitmap(int capacity)
        {
            _capacity = capacity;
            // Calculate how many ulongs are needed to cover 'capacity' bits
            int ulongCount = (capacity + 63) >> 6;
            _buffer = ArrayPool<ulong>.Shared.Rent(ulongCount);

            // IMPORTANT: Arrays from ArrayPool are "dirty", we need to clear them.
            Array.Clear(_buffer, 0, ulongCount);
        }

        /// <summary>
        /// Checks if the bit at the specified index is set (meaning the value is null).
        /// </summary>
        /// <param name="index">The zero-based index to check.</param>
        /// <returns><c>true</c> if the bit is set (null); otherwise, <c>false</c>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsNull(int index)
        {
            // index >> 6 is identical to index / 64, but often faster
            // index & 63 is identical to index % 64
            return (_buffer[index >> 6] & (1UL << (index & 63))) != 0;
        }

        /// <summary>
        /// Sets the bit at the specified index (marking the value as null).
        /// </summary>
        /// <param name="index">The zero-based index to set.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetNull(int index)
        {
            _buffer[index >> 6] |= (1UL << (index & 63));
        }

        /// <summary>
        /// Clears the bit at the specified index (marking the value as not null).
        /// </summary>
        /// <param name="index">The zero-based index to clear.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetNotNull(int index)
        {
            _buffer[index >> 6] &= ~(1UL << (index & 63));
        }

        /// <summary>
        /// Resizes the internal buffer to accommodate at least the specified number of bits.
        /// Preserves existing data.
        /// </summary>
        /// <param name="newCapacity">The new minimum capacity.</param>
        public void Resize(int newCapacity)
        {
            if (newCapacity <= _capacity) return;

            int oldUlongCount = (_capacity + 63) >> 6;
            int newUlongCount = (newCapacity + 63) >> 6;

            // Case 1: Buffer too small -> New buffer needed
            if (newUlongCount > _buffer.Length)
            {
                var newBuffer = ArrayPool<ulong>.Shared.Rent(newUlongCount);

                // Save old data
                Array.Copy(_buffer, newBuffer, oldUlongCount);

                // Clear the new area in the new buffer
                Array.Clear(newBuffer, oldUlongCount, newUlongCount - oldUlongCount);

                ArrayPool<ulong>.Shared.Return(_buffer);
                _buffer = newBuffer;
            }
            // Case 2: Buffer still large enough, but we now use more "words" from it
            else if (newUlongCount > oldUlongCount)
            {
                // Clear the "freshly uncovered" area in the existing dirty buffer
                Array.Clear(_buffer, oldUlongCount, newUlongCount - oldUlongCount);
            }

            _capacity = newCapacity;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (_buffer != null)
            {
                ArrayPool<ulong>.Shared.Return(_buffer);
                _buffer = null!;
            }
        }
    }
}