using System.Buffers;

namespace LeichtFrame.Core
{
    public class StringColumn : Column<string?>, IDisposable
    {
        private string?[] _data;
        private NullBitmap? _nulls;
        private int _length;

        // Interning Strategy
        private readonly bool _useInterning;
        private readonly Dictionary<string, string>? _internPool;

        public StringColumn(string name, int capacity = 16, bool isNullable = false, bool useInterning = false)
            : base(name, isNullable)
        {
            _length = 0;
            _data = ArrayPool<string?>.Shared.Rent(capacity);

            if (isNullable)
            {
                _nulls = new NullBitmap(capacity);
            }

            _useInterning = useInterning;
            if (useInterning)
            {
                _internPool = new Dictionary<string, string>(capacity);
            }
        }

        public override int Length => _length;

        public override ReadOnlyMemory<string?> Values => new ReadOnlyMemory<string?>(_data, 0, _length);

        // --- Core Access ---

        public override string? Get(int index)
        {
            CheckBounds(index);
            if (_nulls != null && _nulls.IsNull(index)) return null;
            return _data[index];
        }

        public override void SetValue(int index, string? value)
        {
            CheckBounds(index);

            if (value == null)
            {
                SetNull(index);
                return;
            }

            if (_useInterning && _internPool != null)
            {
                if (!_internPool.TryGetValue(value, out var interned))
                {
                    _internPool[value] = value;
                    _data[index] = value;
                }
                else
                {
                    _data[index] = interned;
                }
            }
            else
            {
                _data[index] = value;
            }

            _nulls?.SetNotNull(index);
        }

        public override void Append(string? value)
        {
            EnsureCapacity(_length + 1);

            if (value == null)
            {
                if (_nulls == null)
                    throw new InvalidOperationException("Cannot append null to non-nullable column.");

                _data[_length] = null;
                _nulls.SetNull(_length);
            }
            else
            {
                if (_useInterning && _internPool != null)
                {
                    if (!_internPool.TryGetValue(value, out var interned))
                    {
                        _internPool[value] = value;
                        _data[_length] = value;
                    }
                    else
                    {
                        _data[_length] = interned;
                    }
                }
                else
                {
                    _data[_length] = value;
                }

                _nulls?.SetNotNull(_length);
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

            _data[index] = null;
            _nulls.SetNull(index);
        }

        public override void SetNotNull(int index)
        {
            CheckBounds(index);
            _nulls?.SetNotNull(index);
        }

        // --- Memory Estimation ---

        public long EstimateMemoryUsage()
        {
            long total = 0;

            total += _data.Length * IntPtr.Size;

            if (_nulls != null) total += _data.Length / 8;

            for (int i = 0; i < _length; i++)
            {
                var s = _data[i];
                if (s != null)
                {
                    total += 24 + (s.Length * 2);
                }
            }

            if (_internPool != null)
            {
                total += _internPool.Count * 64;
            }

            return total;
        }

        // --- Memory Management ---

        public override void EnsureCapacity(int minCapacity)
        {
            if (_data.Length >= minCapacity) return;

            int newCapacity = Math.Max(_data.Length * 2, minCapacity);

            var newBuffer = ArrayPool<string?>.Shared.Rent(newCapacity);
            Array.Copy(_data, newBuffer, _length);

            Array.Clear(_data, 0, _data.Length);
            ArrayPool<string?>.Shared.Return(_data);

            _data = newBuffer;

            _nulls?.Resize(newCapacity);
        }

        private void CheckBounds(int index)
        {
            if ((uint)index >= (uint)_length) throw new IndexOutOfRangeException();
        }

        public override IColumn CloneSubset(IReadOnlyList<int> indices)
        {
            var newCol = new StringColumn(Name, indices.Count, IsNullable);

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
                Array.Clear(_data, 0, _data.Length);
                ArrayPool<string?>.Shared.Return(_data);
                _data = null!;
            }
            _nulls?.Dispose();
            _nulls = null;
        }
    }
}