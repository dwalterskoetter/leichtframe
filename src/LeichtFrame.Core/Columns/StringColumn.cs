using System;
using System.Buffers;
using System.Collections.Generic;

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

            // Note: Strings from pool might be dirty, but since we manage _length, we just overwrite them.
            // However, strictly cleaning is better for security/debugging, but slower. 
            // We rely on overwrite for active range.

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

        // Note: Returns array containing potential nulls.
        public override ReadOnlyMemory<string?> Values => new ReadOnlyMemory<string?>(_data, 0, _length);

        // --- Core Access ---

        public override string? Get(int index)
        {
            CheckBounds(index);
            // Consistency check: If Bitmap says null, we return null regardless of what's in _data
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

            // Interning Logic
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

        public void Append(string? value)
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

            _data[index] = null; // Remove reference for GC
            _nulls.SetNull(index);
        }

        public override void SetNotNull(int index)
        {
            CheckBounds(index);
            _nulls?.SetNotNull(index);
        }

        // --- Memory Estimation ---

        /// Estimates the memory usage in bytes (Heap size).
        /// Includes array overhead + string content sizes.
        public long EstimateMemoryUsage()
        {
            long total = 0;

            // 1. Array References (8 bytes per pointer on 64-bit)
            total += _data.Length * IntPtr.Size;

            // 2. NullBitmap
            // (Approximated, internal details are hidden but ~ capacity/8)
            if (_nulls != null) total += _data.Length / 8;

            // 3. String Content
            // Overhead per string object ~24 bytes + (Length * 2 bytes for UTF16)
            for (int i = 0; i < _length; i++)
            {
                var s = _data[i];
                if (s != null)
                {
                    total += 24 + (s.Length * 2);
                }
            }

            // 4. Intern Pool overhead (rough estimate)
            if (_internPool != null)
            {
                total += _internPool.Count * 64; // Dict Entry overhead
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

            // Important: Clear old buffer before returning to prevent memory leaks!
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
            // Create new column with exact size (no unnecessary resizing)
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
                    // Get(i) is fast (no boxing)
                    newCol.Append(Get(sourceIndex));
                }
            }
            return newCol;
        }

        public void Dispose()
        {
            if (_data != null)
            {
                // Vital: Clear references so GC can collect the strings
                Array.Clear(_data, 0, _data.Length);
                ArrayPool<string?>.Shared.Return(_data);
                _data = null!;
            }
            _nulls?.Dispose();
            _nulls = null;
        }
    }
}