using System.Buffers;

namespace LeichtFrame.Core
{
    /// <summary>
    /// A high-performance column for storing <see cref="double"/> values.
    /// Supports optimized statistical operations like Sum, Min, and Max using contiguous memory.
    /// </summary>
    public class DoubleColumn : Column<double>, IDisposable
    {
        private double[] _data;
        private NullBitmap? _nulls;
        private int _length;

        /// <summary>
        /// Initializes a new instance of the <see cref="DoubleColumn"/> class.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <param name="capacity">The initial capacity (number of rows).</param>
        /// <param name="isNullable">Whether the column supports null values.</param>
        public DoubleColumn(string name, int capacity = 16, bool isNullable = false)
            : base(name, isNullable)
        {
            _length = 0;
            _data = ArrayPool<double>.Shared.Rent(capacity);
            if (isNullable) _nulls = new NullBitmap(capacity);
        }

        /// <inheritdoc />
        public override int Length => _length;

        /// <inheritdoc />
        public override ReadOnlyMemory<double> Values => new ReadOnlyMemory<double>(_data, 0, _length);

        // --- Core Data Access ---

        /// <inheritdoc />
        public override double Get(int index)
        {
            CheckBounds(index);
            return _data[index];
        }

        /// <inheritdoc />
        public override void SetValue(int index, double value)
        {
            CheckBounds(index);
            _data[index] = value;
            _nulls?.SetNotNull(index);
        }

        /// <inheritdoc />
        public override void Append(double value)
        {
            EnsureCapacity(_length + 1);
            _data[_length] = value;
            _nulls?.SetNotNull(_length);
            _length++;
        }

        /// <summary>
        /// Appends a nullable double value to the column.
        /// </summary>
        /// <param name="value">The value to append, or null.</param>
        /// <exception cref="InvalidOperationException">Thrown if null is passed to a non-nullable column.</exception>
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
                _data[_length] = double.NaN;
                _nulls.SetNull(_length);
            }
            _length++;
        }

        // --- Null Handling ---

        /// <inheritdoc />
        public override bool IsNull(int index)
        {
            CheckBounds(index);
            return _nulls != null && _nulls.IsNull(index);
        }

        /// <inheritdoc />
        public override void SetNull(int index)
        {
            CheckBounds(index);
            if (_nulls == null) throw new InvalidOperationException("Cannot set null on non-nullable column.");
            _data[index] = double.NaN;
            _nulls.SetNull(index);
        }

        /// <inheritdoc />
        public override void SetNotNull(int index)
        {
            CheckBounds(index);
            _nulls?.SetNotNull(index);
        }

        // --- Statistical Helpers ---

        /// <summary>
        /// Calculates the sum of all non-null values in the column.
        /// </summary>
        /// <returns>The sum of values.</returns>
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

        /// <summary>
        /// Finds the minimum value in the column. Ignores null values.
        /// Returns 0 if the column is empty or contains only nulls.
        /// </summary>
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
            return hasValue ? min : 0;
        }

        /// <summary>
        /// Finds the maximum value in the column. Ignores null values.
        /// Returns 0 if the column is empty or contains only nulls.
        /// </summary>
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

        /// <inheritdoc />
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

        /// <inheritdoc />
        public override IColumn CloneSubset(IReadOnlyList<int> indices)
        {
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
                    newCol.Append(Get(sourceIndex));
                }
            }
            return newCol;
        }

        /// <inheritdoc />
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