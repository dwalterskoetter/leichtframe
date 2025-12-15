using LeichtFrame.Core.Internal;

namespace LeichtFrame.Core
{
    /// <summary>
    /// Provides extension methods for grouping operations on <see cref="DataFrame"/>.
    /// Optimized using generic type dispatching and CSR (Compressed Sparse Row) storage.
    /// </summary>
    public static class DataFrameGroupingExtensions
    {
        /// <summary>
        /// Groups the rows of the DataFrame by the values in the specified column.
        /// Uses a high-performance hash map approach converting results to CSR format.
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="columnName">The name of the column to group by.</param>
        /// <returns>A <see cref="GroupedDataFrame"/> object used to apply aggregations.</returns>
        /// <exception cref="ArgumentNullException">Thrown if columnName is null or empty.</exception>
        public static GroupedDataFrame GroupBy(this DataFrame df, string columnName)
        {
            if (string.IsNullOrEmpty(columnName)) throw new ArgumentNullException(nameof(columnName));

            var col = df[columnName];
            Type t = Nullable.GetUnderlyingType(col.DataType) ?? col.DataType;

            // 1. Primitive Optimized Path
            if (t == typeof(int)) return GroupByPrimitive<int>(df, columnName);
            if (t == typeof(double)) return GroupByPrimitive<double>(df, columnName);
            if (t == typeof(long)) return GroupByPrimitive<long>(df, columnName);
            if (t == typeof(bool)) return GroupByPrimitive<bool>(df, columnName);
            if (t == typeof(DateTime)) return GroupByPrimitive<DateTime>(df, columnName);

            // 2. String Optimized Path
            if (t == typeof(string)) return GroupByString(df, columnName);

            throw new NotSupportedException($"GroupBy not yet implemented for type {t.Name} with CSR backend.");
        }

        private static GroupedDataFrame GroupByPrimitive<T>(DataFrame df, string columnName)
            where T : unmanaged, IEquatable<T>
        {
            var col = (IColumn<T>)df[columnName];

            // Use PrimitiveKeyMap
            // Heuristic: Start smaller to save RAM, resize if needed
            var map = new PrimitiveKeyMap<T>(Math.Max(128, df.RowCount / 10), df.RowCount);
            var nullIndices = new List<int>();

            for (int i = 0; i < df.RowCount; i++)
            {
                if (col.IsNull(i))
                {
                    nullIndices.Add(i);
                    continue;
                }
                map.AddRow(col.GetValue(i), i);
            }

            var csr = map.ToCSR();
            map.Dispose();

            // Create GroupedDataFrame with CSR + Null Indices
            return new GroupedDataFrame<T>(
                df,
                columnName,
                csr.Keys,
                csr.GroupOffsets,
                csr.RowIndices,
                nullIndices.Count > 0 ? nullIndices.ToArray() : null
            );
        }

        private static GroupedDataFrame GroupByString(DataFrame df, string columnName)
        {
            var col = (StringColumn)df[columnName];

            // Use StringKeyMap
            var map = new StringKeyMap(col.RawBytes, col.Offsets, Math.Max(128, df.RowCount / 10), df.RowCount);
            var nullIndices = new List<int>();

            for (int i = 0; i < df.RowCount; i++)
            {
                if (col.IsNull(i))
                {
                    nullIndices.Add(i);
                    continue;
                }
                map.AddRow(i);
            }

            var csr = map.ToCSR();
            map.Dispose();

            return new GroupedDataFrame<string>(
                df,
                columnName,
                csr.Keys,
                csr.Offsets,
                csr.Indices,
                nullIndices.Count > 0 ? nullIndices.ToArray() : null
            );
        }

        /// <summary>
        /// Internal helper to unwrap potential wrapper objects used in legacy code.
        /// Currently mainly a pass-through in the generic CSR context.
        /// </summary>
        internal static object? GetRealValue(object key) => key;
    }
}