using System.Buffers;
using LeichtFrame.Core.Engine.Memory;

namespace LeichtFrame.Core
{
    /// <summary>
    /// A high-performance column for storing <see cref="DateTime"/> values.
    /// Uses pooled arrays for zero-allocation data management.
    /// </summary>
    public class DateTimeColumn : Column<DateTime>, IDisposable
    {
        private DateTime[] _data;
        private NullBitmap? _nulls;
        private int _length;

        /// <summary>
        /// Initializes a new instance of the <see cref="DateTimeColumn"/> class.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <param name="capacity">The initial capacity (number of rows).</param>
        /// <param name="isNullable">Whether the column supports null values.</param>
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

        /// <inheritdoc />
        public override int Length => _length;

        /// <inheritdoc />
        public override ReadOnlyMemory<DateTime> Values => new ReadOnlyMemory<DateTime>(_data, 0, _length);

        // --- Core Access ---

        /// <inheritdoc />
        public override DateTime Get(int index)
        {
            CheckBounds(index);
            return _data[index];
        }

        /// <inheritdoc />
        public override void SetValue(int index, DateTime value)
        {
            CheckBounds(index);
            _data[index] = value;
            _nulls?.SetNotNull(index);
        }

        /// <inheritdoc />
        public override void Append(DateTime value)
        {
            EnsureCapacity(_length + 1);
            _data[_length] = value;
            _nulls?.SetNotNull(_length);
            _length++;
        }

        /// <summary>
        /// Appends a nullable DateTime value.
        /// </summary>
        /// <param name="value">The value to append, or null.</param>
        /// <exception cref="InvalidOperationException">Thrown if null is passed to a non-nullable column.</exception>
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

                _data[_length] = default;
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
            if (_nulls == null)
                throw new InvalidOperationException("Cannot set null on non-nullable column.");

            _data[index] = default;
            _nulls.SetNull(index);
        }

        /// <inheritdoc />
        public override void SetNotNull(int index)
        {
            CheckBounds(index);
            _nulls?.SetNotNull(index);
        }

        // --- Memory Management ---

        /// <inheritdoc />
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

        /// <inheritdoc />
        public override IColumn CloneSubset(IReadOnlyList<int> indices)
        {
            var newCol = new DateTimeColumn(Name, indices.Count, IsNullable);

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
                ArrayPool<DateTime>.Shared.Return(_data);
                _data = null!;
            }
            _nulls?.Dispose();
            _nulls = null;
        }
    }
}