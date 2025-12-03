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
            int byteCount = (capacity + 7) >> 3;
            _data = ArrayPool<byte>.Shared.Rent(byteCount);
            Array.Clear(_data, 0, byteCount);

            if (isNullable)
            {
                _nulls = new NullBitmap(capacity);
            }
        }

        public override int Length => _length;

        public override ReadOnlyMemory<bool> Values => throw new NotSupportedException(
            "BoolColumn uses bit-packed storage. Cannot return ReadOnlyMemory<bool>. Use GetValue or dedicated bulk methods.");

        // --- Core Data Access ---

        public override bool Get(int index)
        {
            CheckBounds(index);
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

        public override void Append(bool value)
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
                SetBit(_length, false);
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
            SetBit(index, false);
            _nulls.SetNull(index);
        }

        public override void SetNotNull(int index)
        {
            CheckBounds(index);
            _nulls?.SetNotNull(index);
        }

        // --- Bulk Operations ---

        public bool AnyTrue()
        {
            if (_nulls == null)
            {
                int fullBytes = _length >> 3;
                for (int i = 0; i < fullBytes; i++)
                {
                    if (_data[i] != 0) return true;
                }
                for (int i = fullBytes * 8; i < _length; i++)
                {
                    if (Get(i)) return true;
                }
                return false;
            }
            else
            {
                for (int i = 0; i < _length; i++)
                {
                    if (!IsNull(i) && Get(i)) return true;
                }
                return false;
            }
        }

        public bool AllTrue()
        {
            if (_length == 0) return true;

            if (_nulls == null)
            {
                int fullBytes = _length >> 3;
                for (int i = 0; i < fullBytes; i++)
                {
                    if (_data[i] != 0xFF) return false;
                }
                for (int i = fullBytes * 8; i < _length; i++)
                {
                    if (!Get(i)) return false;
                }
                return true;
            }
            else
            {
                for (int i = 0; i < _length; i++)
                {
                    if (!IsNull(i) && !Get(i)) return false;
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

            int newByteCap = Math.Max(currentByteCap * 2, requiredByteCap);

            var newBuffer = ArrayPool<byte>.Shared.Rent(newByteCap);

            Array.Copy(_data, newBuffer, (Length + 7) >> 3);
            Array.Clear(newBuffer, (Length + 7) >> 3, newByteCap - ((Length + 7) >> 3));

            ArrayPool<byte>.Shared.Return(_data);
            _data = newBuffer;

            _nulls?.Resize(minCapacity);
        }

        private void CheckBounds(int index)
        {
            if ((uint)index >= (uint)_length) throw new IndexOutOfRangeException();
        }

        public override IColumn CloneSubset(IReadOnlyList<int> indices)
        {
            var newCol = new BoolColumn(Name, indices.Count, IsNullable);

            for (int i = 0; i < indices.Count; i++)
            {
                int sourceIndex = indices[i];
                if (IsNullable && IsNull(sourceIndex))
                {
                    newCol.Append(null);
                }
                else
                {
                    newCol.Append(Get(sourceIndex));
                }
            }
            return newCol;
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