using LeichtFrame.Core.Engine;

namespace LeichtFrame.Core.Logic
{
    /// <summary>
    /// Fallback strategy for types that don't have specialized high-performance solvers (like Double, Long, Bool, DateTime).
    /// Uses a high-performance Dictionary (PrimitiveKeyMap) from the Engine.
    /// </summary>
    internal class GenericHashMapStrategy : IGroupByStrategy
    {
        public GroupedDataFrame Group(DataFrame df, string columnName)
        {
            var col = df[columnName];
            Type t = Nullable.GetUnderlyingType(col.DataType) ?? col.DataType;

            if (t == typeof(int)) return GroupT<int>(df, columnName);
            if (t == typeof(double)) return GroupT<double>(df, columnName);
            if (t == typeof(long)) return GroupT<long>(df, columnName);
            if (t == typeof(bool)) return GroupT<bool>(df, columnName);
            if (t == typeof(DateTime)) return GroupT<DateTime>(df, columnName);

            throw new NotSupportedException($"Type {t.Name} not supported in fallback strategy.");
        }

        private GroupedDataFrame GroupT<T>(DataFrame df, string columnName) where T : unmanaged, IEquatable<T>
        {
            var col = (IColumn<T>)df[columnName];

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

            return new GroupedDataFrame<T>(
                df,
                columnName,
                csr.Keys,
                csr.GroupOffsets,
                csr.RowIndices,
                nullIndices.Count > 0 ? nullIndices.ToArray() : null
            );
        }
    }
}