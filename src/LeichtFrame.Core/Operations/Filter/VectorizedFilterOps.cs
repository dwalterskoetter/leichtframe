using System.Numerics;
using System.Runtime.InteropServices;

namespace LeichtFrame.Core.Operations.Filter
{
    /// <summary>
    /// Provides high-performance, SIMD-accelerated filtering operations for DataFrames.
    /// </summary>
    public static class VectorizedFilterOps
    {
        /// <summary>
        /// Filters the DataFrame using hardware-accelerated SIMD instructions.
        /// Supports primitive types like int, double, float.
        /// </summary>
        /// <typeparam name="T">The type of the column (must be an INumber).</typeparam>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="columnName">The name of the column to filter by.</param>
        /// <param name="op">The comparison operator.</param>
        /// <param name="value">The value to compare against.</param>
        /// <returns>A new DataFrame containing only the matching rows.</returns>
        public static DataFrame WhereVec<T>(this DataFrame df, string columnName, CompareOp op, T value)
            where T : struct, INumber<T>
        {
            var col = df[columnName];
            if (col is not IColumn<T> typedCol)
            {
                throw new ArgumentException($"Column '{columnName}' is not of type {typeof(T).Name}");
            }

            ReadOnlySpan<T> data = typedCol.AsSpan();
            var indices = new List<int>(data.Length / 4);

            int i = 0;

            if (Vector.IsHardwareAccelerated)
            {
                int vectorSize = Vector<T>.Count;
                var valueVec = new Vector<T>(value);

                var vectorSpan = MemoryMarshal.Cast<T, Vector<T>>(data);

                for (int vIdx = 0; vIdx < vectorSpan.Length; vIdx++)
                {
                    var dataVec = vectorSpan[vIdx];
                    Vector<T> resultVec;

                    switch (op)
                    {
                        case CompareOp.Equal:
                            resultVec = Vector.Equals(dataVec, valueVec);
                            break;
                        case CompareOp.GreaterThan:
                            resultVec = Vector.GreaterThan(dataVec, valueVec);
                            break;
                        case CompareOp.GreaterThanOrEqual:
                            resultVec = Vector.GreaterThanOrEqual(dataVec, valueVec);
                            break;
                        case CompareOp.LessThan:
                            resultVec = Vector.LessThan(dataVec, valueVec);
                            break;
                        case CompareOp.LessThanOrEqual:
                            resultVec = Vector.LessThanOrEqual(dataVec, valueVec);
                            break;
                        case CompareOp.NotEqual:
                            resultVec = Vector.OnesComplement(Vector.Equals(dataVec, valueVec));
                            break;
                        default:
                            throw new NotSupportedException($"Operator {op} not supported.");
                    }

                    if (resultVec == Vector<T>.Zero)
                    {
                        i += vectorSize;
                        continue;
                    }

                    for (int k = 0; k < vectorSize; k++)
                    {
                        if (resultVec[k] != T.Zero)
                        {
                            int absoluteIndex = i + k;
                            if (!col.IsNullable || !col.IsNull(absoluteIndex))
                            {
                                indices.Add(absoluteIndex);
                            }
                        }
                    }
                    i += vectorSize;
                }
            }

            for (; i < data.Length; i++)
            {
                T val = data[i];
                if (col.IsNullable && col.IsNull(i)) continue;

                bool match = op switch
                {
                    CompareOp.Equal => val == value,
                    CompareOp.NotEqual => val != value,
                    CompareOp.GreaterThan => val > value,
                    CompareOp.GreaterThanOrEqual => val >= value,
                    CompareOp.LessThan => val < value,
                    CompareOp.LessThanOrEqual => val <= value,
                    _ => false
                };

                if (match) indices.Add(i);
            }

            var newColumns = new List<IColumn>();
            foreach (var originalCol in df.Columns)
            {
                newColumns.Add(originalCol.CloneSubset(indices));
            }

            return new DataFrame(newColumns);
        }

        /// <summary>
        /// Specialized vectorized filter for DateTime columns.
        /// </summary>
        public static DataFrame WhereVec(this DataFrame df, string columnName, CompareOp op, DateTime value)
        {
            var col = df[columnName];
            if (col is not DateTimeColumn dtCol)
            {
                throw new ArgumentException($"Column '{columnName}' is not a DateTimeColumn.");
            }

            var data = dtCol.Values.Span;
            var indices = new List<int>(data.Length / 4);

            // Scalar Loop Optimization (DateTime comparison is cheap)
            // Future Optimization: Cast<DateTime, long> and use SIMD on Ticks
            for (int i = 0; i < data.Length; i++)
            {
                // Null Check
                if (dtCol.IsNullable && dtCol.IsNull(i)) continue;

                var val = data[i];
                bool match = op switch
                {
                    CompareOp.Equal => val == value,
                    CompareOp.NotEqual => val != value,
                    CompareOp.GreaterThan => val > value,
                    CompareOp.GreaterThanOrEqual => val >= value,
                    CompareOp.LessThan => val < value,
                    CompareOp.LessThanOrEqual => val <= value,
                    _ => false
                };

                if (match) indices.Add(i);
            }

            // Materialize Result
            var newColumns = new List<IColumn>(df.ColumnCount);
            foreach (var originalCol in df.Columns)
            {
                newColumns.Add(originalCol.CloneSubset(indices));
            }

            return new DataFrame(newColumns);
        }
    }
}