using System.Buffers;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;

namespace LeichtFrame.Core
{
    /// <summary>
    /// A high-performance column for storing <see cref="int"/> values.
    /// Uses pooled arrays for zero-allocation data management.
    /// </summary>
    public class IntColumn : Column<int>, IDisposable
    {
        private int[] _data;
        private NullBitmap? _nulls;
        private int _length;

        /// <summary>
        /// Initializes a new instance of the <see cref="IntColumn"/> class.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <param name="capacity">The initial capacity (number of rows).</param>
        /// <param name="isNullable">Whether the column supports null values.</param>
        public IntColumn(string name, int capacity = 16, bool isNullable = false)
            : base(name, isNullable)
        {
            _length = 0;
            _data = ArrayPool<int>.Shared.Rent(capacity);

            if (isNullable)
            {
                _nulls = new NullBitmap(capacity);
            }
        }

        /// <inheritdoc />
        public override int Length => _length;

        /// <inheritdoc />
        public override ReadOnlyMemory<int> Values => new ReadOnlyMemory<int>(_data, 0, _length);

        // --- Core Get/Set ---

        /// <inheritdoc />
        public override int Get(int index)
        {
            CheckBounds(index);
            return _data[index];
        }

        /// <inheritdoc />
        public override void SetValue(int index, int value)
        {
            CheckBounds(index);
            _data[index] = value;
            _nulls?.SetNotNull(index);
        }

        // --- Append ---

        /// <inheritdoc />
        public override void Append(int value)
        {
            EnsureCapacity(_length + 1);
            _data[_length] = value;
            _nulls?.SetNotNull(_length);
            _length++;
        }

        /// <summary>
        /// Appends a nullable integer value to the column.
        /// </summary>
        /// <param name="value">The value to append, or null.</param>
        /// <exception cref="InvalidOperationException">Thrown if null is passed to a non-nullable column.</exception>
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
                if (_nulls == null)
                    throw new InvalidOperationException("Cannot append null to a non-nullable column.");

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
                throw new InvalidOperationException("Cannot set null on a non-nullable column.");

            _data[index] = default;
            _nulls.SetNull(index);
        }

        /// <inheritdoc />
        public override void SetNotNull(int index)
        {
            CheckBounds(index);
            _nulls?.SetNotNull(index);
        }

        // --- SIMD Aggregations ---

        /// <summary>
        /// Calculates the sum of the column using SIMD with overflow protection (extends to long).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long Sum()
        {
            var span = Values.Span;
            long sum = 0;

            if (Vector.IsHardwareAccelerated && span.Length >= Vector<int>.Count)
            {
                var vectors = MemoryMarshal.Cast<int, Vector<int>>(span);

                // We utilize Vector<long> to accumulate sums to prevent 32-bit overflow.
                var accVectorLow = Vector<long>.Zero;
                var accVectorHigh = Vector<long>.Zero;

                foreach (var v in vectors)
                {
                    // Widen: Split Vector<int> into two Vector<long>
                    Vector.Widen(v, out var low, out var high);
                    accVectorLow += low;
                    accVectorHigh += high;
                }

                // Sum up the lanes of the long accumulators
                sum += Vector.Sum(accVectorLow);
                sum += Vector.Sum(accVectorHigh);

                // Handle remaining elements (Tail)
                int processed = vectors.Length * Vector<int>.Count;
                span = span.Slice(processed);
            }

            // Scalar fallback / Tail loop
            foreach (var val in span)
            {
                sum += val;
            }

            return sum;
        }

        /// <summary>
        /// Calculates the minimum value using SIMD (only for non-nullable).
        /// </summary>
        public int Min()
        {
            if (_length == 0) return 0;

            // SIMD is only safe for non-nullable columns because '0' (null representation) 
            // would falsify the Min calculation.
            if (IsNullable)
            {
                return MinNullable();
            }

            var span = Values.Span;
            int min = int.MaxValue;

            if (Vector.IsHardwareAccelerated && span.Length >= Vector<int>.Count)
            {
                var vectors = MemoryMarshal.Cast<int, Vector<int>>(span);
                var minVector = new Vector<int>(int.MaxValue);

                foreach (var v in vectors)
                {
                    minVector = Vector.Min(minVector, v);
                }

                // Reduce vector lanes
                for (int i = 0; i < Vector<int>.Count; i++)
                {
                    if (minVector[i] < min) min = minVector[i];
                }

                int processed = vectors.Length * Vector<int>.Count;
                span = span.Slice(processed);
            }

            foreach (var val in span)
            {
                if (val < min) min = val;
            }

            return min;
        }

        /// <summary>
        /// Calculates the maximum value using SIMD (only for non-nullable).
        /// </summary>
        public int Max()
        {
            if (_length == 0) return 0;

            if (IsNullable)
            {
                return MaxNullable();
            }

            var span = Values.Span;
            int max = int.MinValue;

            if (Vector.IsHardwareAccelerated && span.Length >= Vector<int>.Count)
            {
                var vectors = MemoryMarshal.Cast<int, Vector<int>>(span);
                var maxVector = new Vector<int>(int.MinValue);

                foreach (var v in vectors)
                {
                    maxVector = Vector.Max(maxVector, v);
                }

                for (int i = 0; i < Vector<int>.Count; i++)
                {
                    if (maxVector[i] > max) max = maxVector[i];
                }

                int processed = vectors.Length * Vector<int>.Count;
                span = span.Slice(processed);
            }

            foreach (var val in span)
            {
                if (val > max) max = val;
            }

            return max;
        }

        // Fallback helpers for Nullable types (Scalar loop with null checks)
        private int MinNullable()
        {
            int min = int.MaxValue;
            bool hasValue = false;
            for (int i = 0; i < _length; i++)
            {
                if (!IsNull(i))
                {
                    int val = _data[i];
                    if (val < min) min = val;
                    hasValue = true;
                }
            }
            return hasValue ? min : 0;
        }

        private int MaxNullable()
        {
            int max = int.MinValue;
            bool hasValue = false;
            for (int i = 0; i < _length; i++)
            {
                if (!IsNull(i))
                {
                    int val = _data[i];
                    if (val > max) max = val;
                    hasValue = true;
                }
            }
            return hasValue ? max : 0;
        }

        // --- Memory Management ---

        /// <inheritdoc />
        public override void EnsureCapacity(int minCapacity)
        {
            if (_data.Length >= minCapacity) return;

            int newCapacity = Math.Max(_data.Length * 2, minCapacity);

            var newBuffer = ArrayPool<int>.Shared.Rent(newCapacity);
            Array.Copy(_data, newBuffer, _length);
            ArrayPool<int>.Shared.Return(_data);
            _data = newBuffer;

            _nulls?.Resize(newCapacity);
        }

        private void CheckBounds(int index)
        {
            if ((uint)index >= (uint)_length)
                throw new IndexOutOfRangeException($"Index {index} is out of range (Length: {_length})");
        }

        /// <inheritdoc />
        public override IColumn CloneSubset(IReadOnlyList<int> indices)
        {
            var newCol = new IntColumn(Name, indices.Count, IsNullable);

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
                ArrayPool<int>.Shared.Return(_data);
                _data = null!;
            }

            _nulls?.Dispose();
            _nulls = null;
        }

        // --- Arithmetic Operators ---

        /// <summary>
        /// Adds two integer columns element-wise.
        /// </summary>
        public static IntColumn operator +(IntColumn a, IntColumn b) => ExecuteOp(a, b, VectorizedMathOps.MathOp.Add);

        /// <summary>
        /// Subtracts the second integer column from the first element-wise.
        /// </summary>
        public static IntColumn operator -(IntColumn a, IntColumn b) => ExecuteOp(a, b, VectorizedMathOps.MathOp.Subtract);

        /// <summary>
        /// Multiplies two integer columns element-wise.
        /// </summary>
        public static IntColumn operator *(IntColumn a, IntColumn b) => ExecuteOp(a, b, VectorizedMathOps.MathOp.Multiply);

        /// <summary>
        /// Divides the first integer column by the second element-wise.
        /// </summary>
        public static IntColumn operator /(IntColumn a, IntColumn b) => ExecuteOp(a, b, VectorizedMathOps.MathOp.Divide);

        /// <summary>
        /// Adds a scalar value to every element in the column.
        /// </summary>
        public static IntColumn operator +(IntColumn a, int b) => ExecuteOpScalar(a, b, VectorizedMathOps.MathOp.Add);

        /// <summary>
        /// Subtracts a scalar value from every element in the column.
        /// </summary>
        public static IntColumn operator -(IntColumn a, int b) => ExecuteOpScalar(a, b, VectorizedMathOps.MathOp.Subtract);

        /// <summary>
        /// Multiplies every element in the column by a scalar value.
        /// </summary>
        public static IntColumn operator *(IntColumn a, int b) => ExecuteOpScalar(a, b, VectorizedMathOps.MathOp.Multiply);

        /// <summary>
        /// Divides every element in the column by a scalar value.
        /// </summary>
        public static IntColumn operator /(IntColumn a, int b) => ExecuteOpScalar(a, b, VectorizedMathOps.MathOp.Divide);

        private static IntColumn ExecuteOp(IntColumn a, IntColumn b, VectorizedMathOps.MathOp op)
        {
            if (a.Length != b.Length) throw new ArgumentException("Column lengths mismatch");

            bool resultNullable = a.IsNullable || b.IsNullable;
            var result = new IntColumn($"{a.Name}_op_{b.Name}", a.Length, resultNullable);

            result._length = a.Length;
            VectorizedMathOps.Calculate<int>(a._data.AsSpan(0, a.Length), b._data.AsSpan(0, b.Length), result._data.AsSpan(0, a.Length), op);

            if (resultNullable)
            {
                result._nulls?.Dispose();
                result._nulls = NullBitmap.MergeOr(a._nulls, b._nulls, a.Length);
            }
            return result;
        }

        private static IntColumn ExecuteOpScalar(IntColumn a, int scalar, VectorizedMathOps.MathOp op)
        {
            var result = new IntColumn($"{a.Name}_op_scalar", a.Length, a.IsNullable);
            result._length = a.Length;

            VectorizedMathOps.CalculateScalar<int>(a._data.AsSpan(0, a.Length), scalar, result._data.AsSpan(0, a.Length), op);

            if (a.IsNullable && a._nulls != null)
            {
                result._nulls?.Dispose();
                result._nulls = NullBitmap.MergeOr(a._nulls, null, a.Length);
            }
            return result;
        }
    }
}