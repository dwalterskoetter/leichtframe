using System;
using System.Buffers;

namespace LeichtFrame.Core
{
    public class IntColumn : Column<int>, IDisposable
    {
        private int[] _data;
        private NullBitmap? _nulls; // Null only if column is not nullable
        private int _length;

        /// Creates a new IntColumn. 
        /// Note: If you updated the base Column class to accept IsNullable, pass it to base().
        public IntColumn(string name, int capacity = 16, bool isNullable = false)
            : base(name, isNullable)
        {
            _length = 0;
            // Rent from pool to avoid GC pressure
            _data = ArrayPool<int>.Shared.Rent(capacity);

            // Non-nullable columns do not allocate the bitmap
            if (isNullable)
            {
                _nulls = new NullBitmap(capacity);
            }
        }

        public override int Length => _length;

        /// Returns a ReadOnlyMemory slice of the valid data. Zero-copy.
        public override ReadOnlyMemory<int> Values => new ReadOnlyMemory<int>(_data, 0, _length);

        // --- Core Get/Set ---

        public override int GetValue(int index)
        {
            CheckBounds(index);
            // In a real scenario, you might want to check IsNull(index) here 
            // and throw or return default, depending on policy. 
            // For raw access, we return the value in the buffer.
            return _data[index];
        }

        public override void SetValue(int index, int value)
        {
            CheckBounds(index);
            _data[index] = value;
            // If we have a bitmap, we must explicitly mark this index as NOT null
            _nulls?.SetNotNull(index);
        }

        // --- Append / Builder API ---

        public void Append(int value)
        {
            EnsureCapacity(_length + 1);
            _data[_length] = value;
            _nulls?.SetNotNull(_length);
            _length++;
        }

        public void Append(int? value)
        {
            EnsureCapacity(_length + 1);

            if (value.HasValue)
            {
                _data[_length] = value.Value;
                _nulls?.SetNotNull(_length);
            }
            else
            {
                // Verify column supports nulls
                if (_nulls == null)
                    throw new InvalidOperationException("Cannot append null to a non-nullable column.");

                _data[_length] = default; // 0
                _nulls.SetNull(_length);
            }
            _length++;
        }

        // --- Null Handling ---
        public override bool IsNull(int index)
        {
            CheckBounds(index);
            // If _nulls is null, the column is non-nullable, so value is never null.
            return _nulls != null && _nulls.IsNull(index);
        }

        public override void SetNull(int index)
        {
            CheckBounds(index);
            if (_nulls == null)
                throw new InvalidOperationException("Cannot set null on a non-nullable column.");

            _data[index] = default; // Clear value for safety/compression
            _nulls.SetNull(index);
        }

        public override void SetNotNull(int index)
        {
            CheckBounds(index);
            _nulls?.SetNotNull(index);
        }

        // --- Memory Management ---
        public override void EnsureCapacity(int minCapacity)
        {
            if (_data.Length >= minCapacity) return;

            // Growth strategy: Double or match requested
            int newCapacity = Math.Max(_data.Length * 2, minCapacity);

            // 1. Resize Data Array
            var newBuffer = ArrayPool<int>.Shared.Rent(newCapacity);
            Array.Copy(_data, newBuffer, _length);
            ArrayPool<int>.Shared.Return(_data); // Return old
            _data = newBuffer;

            // 2. Resize NullBitmap (only if it exists)
            _nulls?.Resize(newCapacity);
        }

        private void CheckBounds(int index)
        {
            if ((uint)index >= (uint)_length) // Optimized check (handles negative too)
                throw new IndexOutOfRangeException($"Index {index} is out of range (Length: {_length})");
        }

        public void Dispose()
        {
            if (_data != null)
            {
                ArrayPool<int>.Shared.Return(_data);
                _data = null!;
            }

            _nulls?.Dispose();
            _nulls = null;
        }
    }
}