using System;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace LeichtFrame.Core
{
    public class BoolColumn : Column<bool>, IDisposable
    {
        private byte[] _data; // 8 bools per byte
        private NullBitmap? _nulls;
        private int _length;

        public BoolColumn(string name, int capacity = 16, bool isNullable = false)
            : base(name, isNullable)
        {
            _length = 0;
            // Capacity / 8, round up
            int byteCount = (capacity + 7) >> 3;
            _data = ArrayPool<byte>.Shared.Rent(byteCount);
            Array.Clear(_data, 0, byteCount); // Vital: Pool arrays are dirty

            if (isNullable)
            {
                _nulls = new NullBitmap(capacity);
            }
        }

        public override int Length => _length;

        /// WARNING: Not supported for BoolColumn because data is bit-packed (1 bit per bool).
        /// Returning a ReadOnlyMemory<bool> would require unpacking/copying, violating zero-copy principles.
        /// Use GetValue(i) or AllTrue/AnyTrue instead.
        public override ReadOnlyMemory<bool> Values => throw new NotSupportedException(
            "BoolColumn uses bit-packed storage. Cannot return ReadOnlyMemory<bool>. Use GetValue or dedicated bulk methods.");

        // --- Core Data Access ---

        public override bool GetValue(int index)
        {
            CheckBounds(index);
            // byteIndex = index / 8, bitIndex = index % 8
            return (_data[index >> 3] & (1 << (index & 7))) != 0;
        }

        public override void SetValue(int index, bool value)
        {
            CheckBounds(index);
            SetBit(index, value);
            _nulls?.SetNotNull(index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SetBit(int index, bool value)
        {
            int byteIndex = index >> 3;
            int bitMask = 1 << (index & 7);

            if (value)
                _data[byteIndex] |= (byte)bitMask;
            else
                _data[byteIndex] &= (byte)~bitMask;
        }

        public void Append(bool value)
        {
            EnsureCapacity(_length + 1);
            SetBit(_length, value);
            _nulls?.SetNotNull(_length);
            _length++;
        }

        public void Append(bool? value)
        {
            EnsureCapacity(_length + 1);
            if (value.HasValue)
            {
                SetBit(_length, value.Value);
                _nulls?.SetNotNull(_length);
            }
            else
            {
                if (_nulls == null) throw new InvalidOperationException("Cannot append null to non-nullable column.");
                SetBit(_length, false); // Default to false
                _nulls.SetNull(_length);
            }
            _length++;
        }

        // --- Null Handling ---

        public override bool IsNull(int index)
        {
            CheckBounds(index);
            return _nulls != null && _nulls.IsNull(index);
        }

        public override void SetNull(int index)
        {
            CheckBounds(index);
            if (_nulls == null) throw new InvalidOperationException("Cannot set null on non-nullable column.");
            SetBit(index, false); // Clear bit for consistency
            _nulls.SetNull(index);
        }

        public override void SetNotNull(int index)
        {
            CheckBounds(index);
            _nulls?.SetNotNull(index);
        }

        // --- Bulk Operations ---

        /// Returns true if at least one value is true. Ignores nulls (null != true).
        public bool AnyTrue()
        {
            // Optimization for Non-Nullable: Check bytes directly
            if (_nulls == null)
            {
                int fullBytes = _length >> 3;

                // 1. Check full bytes
                for (int i = 0; i < fullBytes; i++)
                {
                    if (_data[i] != 0) return true;
                }

                // 2. Check remaining bits
                for (int i = fullBytes * 8; i < _length; i++)
                {
                    if (GetValue(i)) return true;
                }
                return false;
            }
            else
            {
                // Slow path for Nullable: Must check !IsNull && Value
                // (Could be optimized with bitwise ops between _data and _nulls, but complex due to byte vs ulong mismatch)
                for (int i = 0; i < _length; i++)
                {
                    if (!IsNull(i) && GetValue(i)) return true;
                }
                return false;
            }
        }

        /// Returns true if ALL values are true. Ignores nulls (skips them). 
        /// Returns true if collection is empty.
        public bool AllTrue()
        {
            if (_length == 0) return true;

            // Optimization for Non-Nullable
            if (_nulls == null)
            {
                int fullBytes = _length >> 3;

                // 1. Check full bytes (must be 0xFF / 255)
                for (int i = 0; i < fullBytes; i++)
                {
                    if (_data[i] != 0xFF) return false;
                }

                // 2. Check remaining bits
                for (int i = fullBytes * 8; i < _length; i++)
                {
                    if (!GetValue(i)) return false;
                }
                return true;
            }
            else
            {
                // Nullable path
                for (int i = 0; i < _length; i++)
                {
                    if (!IsNull(i) && !GetValue(i)) return false;
                }
                return true;
            }
        }

        // --- Memory ---

        public override void EnsureCapacity(int minCapacity)
        {
            int currentByteCap = _data.Length;
            int requiredByteCap = (minCapacity + 7) >> 3;

            if (currentByteCap >= requiredByteCap) return;

            // Double capacity strategy
            int newByteCap = Math.Max(currentByteCap * 2, requiredByteCap);

            var newBuffer = ArrayPool<byte>.Shared.Rent(newByteCap);

            // Copy & Clear
            Array.Copy(_data, newBuffer, (Length + 7) >> 3); // Copy only used bytes
            Array.Clear(newBuffer, (Length + 7) >> 3, newByteCap - ((Length + 7) >> 3)); // Clear rest

            ArrayPool<byte>.Shared.Return(_data);
            _data = newBuffer;

            _nulls?.Resize(minCapacity); // NullBitmap handles its own ulong logic
        }

        private void CheckBounds(int index)
        {
            if ((uint)index >= (uint)_length) throw new IndexOutOfRangeException();
        }

        public void Dispose()
        {
            if (_data != null)
            {
                ArrayPool<byte>.Shared.Return(_data);
                _data = null!;
            }
            _nulls?.Dispose();
            _nulls = null;
        }
    }
}