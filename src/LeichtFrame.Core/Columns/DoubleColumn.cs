using System.Buffers;

namespace LeichtFrame.Core
{
    public class DoubleColumn : Column<double>, IDisposable
    {
        private double[] _data;
        private NullBitmap? _nulls;
        private int _length;

        public DoubleColumn(string name, int capacity = 16, bool isNullable = false)
            : base(name, isNullable)
        {
            _length = 0;
            _data = ArrayPool<double>.Shared.Rent(capacity);
            if (isNullable) _nulls = new NullBitmap(capacity);
        }

        public override int Length => _length;
        public override ReadOnlyMemory<double> Values => new ReadOnlyMemory<double>(_data, 0, _length);

        // --- Core Data Access ---
        public override double Get(int index)
        {
            CheckBounds(index);
            return _data[index];
        }

        public override void SetValue(int index, double value)
        {
            CheckBounds(index);
            _data[index] = value;
            _nulls?.SetNotNull(index);
        }

        public void Append(double value)
        {
            EnsureCapacity(_length + 1);
            _data[_length] = value;
            _nulls?.SetNotNull(_length);
            _length++;
        }

        public void Append(double? value)
        {
            EnsureCapacity(_length + 1);
            if (value.HasValue)
            {
                _data[_length] = value.Value;
                _nulls?.SetNotNull(_length);
            }
            else
            {
                if (_nulls == null) throw new InvalidOperationException("Cannot append null to non-nullable column.");
                _data[_length] = double.NaN; // Visual marker, truth is in bitmap
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
            _data[index] = double.NaN;
            _nulls.SetNull(index);
        }

        public override void SetNotNull(int index)
        {
            CheckBounds(index);
            _nulls?.SetNotNull(index);
        }

        // --- Statistical Helpers ---
        public double Sum()
        {
            double sum = 0;
            var span = Values.Span;

            if (_nulls == null)
            {
                for (int i = 0; i < _length; i++) sum += span[i];
            }
            else
            {
                for (int i = 0; i < _length; i++)
                {
                    if (!_nulls.IsNull(i)) sum += span[i];
                }
            }
            return sum;
        }

        public double Min()
        {
            if (_length == 0) return 0;
            double min = double.MaxValue;
            bool hasValue = false;
            var span = Values.Span;

            for (int i = 0; i < _length; i++)
            {
                if (!IsNull(i))
                {
                    double val = span[i];
                    if (val < min) min = val;
                    hasValue = true;
                }
            }
            return hasValue ? min : 0; // Or NaN/Exception based on policy
        }

        public double Max()
        {
            if (_length == 0) return 0;
            double max = double.MinValue;
            bool hasValue = false;
            var span = Values.Span;

            for (int i = 0; i < _length; i++)
            {
                if (!IsNull(i))
                {
                    double val = span[i];
                    if (val > max) max = val;
                    hasValue = true;
                }
            }
            return hasValue ? max : 0;
        }

        // --- Memory ---
        public override void EnsureCapacity(int minCapacity)
        {
            if (_data.Length >= minCapacity) return;
            int newCapacity = Math.Max(_data.Length * 2, minCapacity);

            var newBuffer = ArrayPool<double>.Shared.Rent(newCapacity);
            Array.Copy(_data, newBuffer, _length);
            ArrayPool<double>.Shared.Return(_data);
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
            var newCol = new DoubleColumn(Name, indices.Count, IsNullable);

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
                ArrayPool<double>.Shared.Return(_data);
                _data = null!;
            }
            _nulls?.Dispose();
            _nulls = null;
        }
    }
}