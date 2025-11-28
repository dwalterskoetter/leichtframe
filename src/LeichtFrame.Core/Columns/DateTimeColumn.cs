using System;
using System.Buffers;

namespace LeichtFrame.Core
{
    public class DateTimeColumn : Column<DateTime>, IDisposable
    {
        private DateTime[] _data;
        private NullBitmap? _nulls;
        private int _length;

        public DateTimeColumn(string name, int capacity = 16, bool isNullable = false)
            : base(name, isNullable)
        {
            _length = 0;
            _data = ArrayPool<DateTime>.Shared.Rent(capacity);

            if (isNullable)
            {
                _nulls = new NullBitmap(capacity);
            }
        }

        public override int Length => _length;

        /// Returns a zero-copy span/memory of the dates.
        public override ReadOnlyMemory<DateTime> Values => new ReadOnlyMemory<DateTime>(_data, 0, _length);

        // --- Core Access ---

        public override DateTime Get(int index)
        {
            CheckBounds(index);
            return _data[index];
        }

        public override void SetValue(int index, DateTime value)
        {
            CheckBounds(index);
            _data[index] = value;
            _nulls?.SetNotNull(index);
        }

        public void Append(DateTime value)
        {
            EnsureCapacity(_length + 1);
            _data[_length] = value;
            _nulls?.SetNotNull(_length);
            _length++;
        }

        public void Append(DateTime? value)
        {
            EnsureCapacity(_length + 1);
            if (value.HasValue)
            {
                _data[_length] = value.Value;
                _nulls?.SetNotNull(_length);
            }
            else
            {
                if (_nulls == null)
                    throw new InvalidOperationException("Cannot append null to non-nullable column.");

                _data[_length] = default; // 0001-01-01
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
            if (_nulls == null)
                throw new InvalidOperationException("Cannot set null on non-nullable column.");

            _data[index] = default;
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

            int newCapacity = Math.Max(_data.Length * 2, minCapacity);

            var newBuffer = ArrayPool<DateTime>.Shared.Rent(newCapacity);
            Array.Copy(_data, newBuffer, _length);

            ArrayPool<DateTime>.Shared.Return(_data);
            _data = newBuffer;

            _nulls?.Resize(newCapacity);
        }

        private void CheckBounds(int index)
        {
            if ((uint)index >= (uint)_length)
                throw new IndexOutOfRangeException($"Index {index} is out of range.");
        }

        public void Dispose()
        {
            if (_data != null)
            {
                ArrayPool<DateTime>.Shared.Return(_data);
                _data = null!;
            }
            _nulls?.Dispose();
            _nulls = null;
        }
    }
}