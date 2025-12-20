using System.Buffers;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;
using LeichtFrame.Core.Engine;
using LeichtFrame.Core.Engine.Memory;

namespace LeichtFrame.Core
{
    /// <summary>
    /// A high-performance column for storing <see cref="double"/> values.
    /// Supports optimized statistical operations like Sum, Min, and Max using contiguous memory and SIMD.
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

        // --- Statistical Helpers (SIMD Optimized) ---

        /// <summary>
        /// Calculates the sum of the column. Uses SIMD for non-nullable columns.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public double Sum()
        {
            // Optimization: If non-nullable, we can use SIMD and ignore null checks.
            // If nullable, we cannot blindly use SIMD because NaN + Value = NaN.
            if (IsNullable)
            {
                return SumNullable();
            }

            var span = Values.Span;
            double sum = 0;

            if (Vector.IsHardwareAccelerated && span.Length >= Vector<double>.Count)
            {
                var vectors = MemoryMarshal.Cast<double, Vector<double>>(span);
                var accVector = Vector<double>.Zero;

                foreach (var v in vectors)
                {
                    accVector += v;
                }

                sum += Vector.Sum(accVector);

                int processed = vectors.Length * Vector<double>.Count;
                span = span.Slice(processed);
            }

            // Tail loop
            foreach (var val in span)
            {
                sum += val;
            }

            return sum;
        }

        private double SumNullable()
        {
            double sum = 0;
            for (int i = 0; i < _length; i++)
            {
                if (!IsNull(i)) sum += _data[i];
            }
            return sum;
        }

        /// <summary>
        /// Finds the minimum value. Optimized for non-nullable.
        /// </summary>
        public double Min()
        {
            if (_length == 0) return 0;
            if (IsNullable) return MinNullable();

            // Non-Nullable Scalar Optimization (Fastest for Doubles due to NaN checks in SIMD being complex)
            var span = Values.Span;
            double min = double.MaxValue;
            foreach (var val in span)
            {
                if (val < min) min = val;
            }
            return min;
        }

        private double MinNullable()
        {
            double min = double.MaxValue;
            bool hasValue = false;
            for (int i = 0; i < _length; i++)
            {
                if (!IsNull(i))
                {
                    double val = _data[i];
                    if (val < min) min = val;
                    hasValue = true;
                }
            }
            return hasValue ? min : 0;
        }

        /// <summary>
        /// Finds the maximum value. Optimized for non-nullable.
        /// </summary>
        public double Max()
        {
            if (_length == 0) return 0;
            if (IsNullable) return MaxNullable();

            // Non-Nullable Scalar Optimization
            var span = Values.Span;
            double max = double.MinValue;
            foreach (var val in span)
            {
                if (val > max) max = val;
            }
            return max;
        }

        private double MaxNullable()
        {
            double max = double.MinValue;
            bool hasValue = false;
            for (int i = 0; i < _length; i++)
            {
                if (!IsNull(i))
                {
                    double val = _data[i];
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

        // --- Arithmetic Operators ---

        /// <summary>
        /// Adds two double columns element-wise.
        /// </summary>
        public static DoubleColumn operator +(DoubleColumn a, DoubleColumn b) => ExecuteOp(a, b, VectorizedMathOps.MathOp.Add);

        /// <summary>
        /// Subtracts the second double column from the first element-wise.
        /// </summary>
        public static DoubleColumn operator -(DoubleColumn a, DoubleColumn b) => ExecuteOp(a, b, VectorizedMathOps.MathOp.Subtract);

        /// <summary>
        /// Multiplies two double columns element-wise.
        /// </summary>
        public static DoubleColumn operator *(DoubleColumn a, DoubleColumn b) => ExecuteOp(a, b, VectorizedMathOps.MathOp.Multiply);

        /// <summary>
        /// Divides the first double column by the second element-wise.
        /// </summary>
        public static DoubleColumn operator /(DoubleColumn a, DoubleColumn b) => ExecuteOp(a, b, VectorizedMathOps.MathOp.Divide);

        /// <summary>
        /// Adds a scalar value to every element in the column.
        /// </summary>
        public static DoubleColumn operator +(DoubleColumn a, double b) => ExecuteOpScalar(a, b, VectorizedMathOps.MathOp.Add);

        /// <summary>
        /// Subtracts a scalar value from every element in the column.
        /// </summary>
        public static DoubleColumn operator -(DoubleColumn a, double b) => ExecuteOpScalar(a, b, VectorizedMathOps.MathOp.Subtract);

        /// <summary>
        /// Multiplies every element in the column by a scalar value.
        /// </summary>
        public static DoubleColumn operator *(DoubleColumn a, double b) => ExecuteOpScalar(a, b, VectorizedMathOps.MathOp.Multiply);

        /// <summary>
        /// Divides every element in the column by a scalar value.
        /// </summary>
        public static DoubleColumn operator /(DoubleColumn a, double b) => ExecuteOpScalar(a, b, VectorizedMathOps.MathOp.Divide);

        private static DoubleColumn ExecuteOp(DoubleColumn a, DoubleColumn b, VectorizedMathOps.MathOp op)
        {
            if (a.Length != b.Length) throw new ArgumentException("Column lengths mismatch");

            bool resultNullable = a.IsNullable || b.IsNullable;
            var result = new DoubleColumn($"{a.Name}_op_{b.Name}", a.Length, resultNullable);

            result._length = a.Length;
            VectorizedMathOps.Calculate<double>(a._data.AsSpan(0, a.Length), b._data.AsSpan(0, b.Length), result._data.AsSpan(0, a.Length), op);

            if (resultNullable)
            {
                result._nulls?.Dispose();
                result._nulls = NullBitmap.MergeOr(a._nulls, b._nulls, a.Length);
            }
            return result;
        }

        private static DoubleColumn ExecuteOpScalar(DoubleColumn a, double scalar, VectorizedMathOps.MathOp op)
        {
            var result = new DoubleColumn($"{a.Name}_op_scalar", a.Length, a.IsNullable);
            result._length = a.Length;

            VectorizedMathOps.CalculateScalar<double>(a._data.AsSpan(0, a.Length), scalar, result._data.AsSpan(0, a.Length), op);

            if (a.IsNullable && a._nulls != null)
            {
                result._nulls?.Dispose();
                result._nulls = NullBitmap.MergeOr(a._nulls, null, a.Length);
            }
            return result;
        }

        /// <summary>
        /// Computes the sum over a subset of indices.
        /// </summary>
        /// <param name="indices"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        public override object? ComputeSum(int[] indices, int start, int end)
        {
            double sum = 0;
            var data = _data;

            if (!IsNullable)
            {
                for (int i = start; i < end; i++) sum += data[indices[i]];
            }
            else
            {
                var nulls = _nulls!;
                for (int i = start; i < end; i++)
                {
                    int idx = indices[i];
                    if (!nulls.IsNull(idx)) sum += data[idx];
                }
            }
            return sum;
        }

        /// <summary>
        /// Computes the mean over a subset of indices.
        /// </summary>
        /// <param name="indices"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        public override object? ComputeMean(int[] indices, int start, int end)
        {
            int count = end - start;
            if (count == 0) return null;

            // Summe berechnen (wir rufen intern die Logik von oben ab oder kopieren sie für Speed)
            double sum = (double)(ComputeSum(indices, start, end) ?? 0.0);
            return sum / count;
        }

        /// <summary>
        /// Computes the minimum over a subset of indices.
        /// </summary>
        /// <param name="indices"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        public override object? ComputeMin(int[] indices, int start, int end)
        {
            if (start == end) return null;
            double min = double.MaxValue;
            bool hasVal = false;
            var data = _data;

            if (!IsNullable)
            {
                for (int i = start; i < end; i++)
                {
                    double v = data[indices[i]];
                    if (v < min) min = v;
                }
                hasVal = true;
            }
            else
            {
                var nulls = _nulls!;
                for (int i = start; i < end; i++)
                {
                    int idx = indices[i];
                    if (!nulls.IsNull(idx))
                    {
                        double v = data[idx];
                        if (v < min) min = v;
                        hasVal = true;
                    }
                }
            }
            return hasVal ? min : null;
        }


        /// <summary>
        /// Computes the maximum over a subset of indices.
        /// </summary>
        /// <param name="indices"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        public override object? ComputeMax(int[] indices, int start, int end)
        {
            if (start == end) return null;
            double max = double.MinValue;
            bool hasVal = false;
            var data = _data;

            if (!IsNullable)
            {
                for (int i = start; i < end; i++)
                {
                    double v = data[indices[i]];
                    if (v > max) max = v;
                }
                hasVal = true;
            }
            else
            {
                var nulls = _nulls!;
                for (int i = start; i < end; i++)
                {
                    int idx = indices[i];
                    if (!nulls.IsNull(idx))
                    {
                        double v = data[idx];
                        if (v > max) max = v;
                        hasVal = true;
                    }
                }
            }
            return hasVal ? max : null;
        }
    }
}