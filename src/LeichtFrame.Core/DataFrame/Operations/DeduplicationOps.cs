namespace LeichtFrame.Core
{
    /// <summary>
    /// Provides extension methods for removing duplicate rows.
    /// </summary>
    public static class DeduplicationOps
    {
        /// <summary>
        /// Returns a new DataFrame containing only unique rows.
        /// </summary>
        public static DataFrame Distinct(this DataFrame df)
        {
            return Distinct(df, df.GetColumnNames().ToArray());
        }

        /// <summary>
        /// Returns a new DataFrame containing rows that are unique based on the specified subset of columns.
        /// </summary>
        public static DataFrame Distinct(this DataFrame df, params string[] columnNames)
        {
            if (columnNames == null || columnNames.Length == 0)
                throw new ArgumentException("At least one column must be specified.");

            // Optimization: Single Column Distinct
            // We can use a typed HashSet<T> to avoid boxing/allocations
            if (columnNames.Length == 1)
            {
                return DistinctSingleColumn(df, columnNames[0]);
            }

            // Fallback: Multi-column slow path
            return DistinctMultiColumn(df, columnNames);
        }

        private static DataFrame DistinctSingleColumn(DataFrame df, string columnName)
        {
            var col = df[columnName];
            Type type = col.DataType;
            Type coreType = Nullable.GetUnderlyingType(type) ?? type;

            if (coreType == typeof(int)) return ExecuteDistinctSingle<int>(df, col);
            if (coreType == typeof(double)) return ExecuteDistinctSingle<double>(df, col);
            if (coreType == typeof(string)) return ExecuteDistinctSingle<string>(df, col);
            if (coreType == typeof(bool)) return ExecuteDistinctSingle<bool>(df, col);
            if (coreType == typeof(DateTime)) return ExecuteDistinctSingle<DateTime>(df, col);

            return ExecuteDistinctSingle<object>(df, col);
        }

        private static DataFrame ExecuteDistinctSingle<T>(DataFrame df, IColumn colUntyped) where T : notnull
        {
            var col = (IColumn<T>)colUntyped;
            var seen = new HashSet<T>();
            var indices = new List<int>(df.RowCount);
            bool nullSeen = false;

            for (int i = 0; i < df.RowCount; i++)
            {
                if (col.IsNull(i))
                {
                    if (!nullSeen)
                    {
                        nullSeen = true;
                        indices.Add(i);
                    }
                    continue;
                }

                T val = col.GetValue(i);

                // HashSet.Add returns true if the element was added (new unique)
                if (val != null && seen.Add(val))
                {
                    indices.Add(i);
                }
                else if (val == null && !nullSeen) // Handle nulls in reference types (strings)
                {
                    nullSeen = true;
                    indices.Add(i);
                }
            }

            return CreateSubset(df, indices);
        }

        private static DataFrame DistinctMultiColumn(DataFrame df, string[] columnNames)
        {
            var colsToCheck = new IColumn[columnNames.Length];
            for (int i = 0; i < columnNames.Length; i++)
            {
                colsToCheck[i] = df[columnNames[i]];
            }

            var comparer = new RowIndexComparer(colsToCheck);
            var seenRows = new HashSet<int>(comparer);
            var uniqueIndices = new List<int>(df.RowCount);

            for (int i = 0; i < df.RowCount; i++)
            {
                if (seenRows.Add(i))
                {
                    uniqueIndices.Add(i);
                }
            }

            return CreateSubset(df, uniqueIndices);
        }

        private static DataFrame CreateSubset(DataFrame df, List<int> indices)
        {
            if (indices.Count == df.RowCount) return df;

            var newColumns = new List<IColumn>(df.ColumnCount);
            foreach (var col in df.Columns)
            {
                newColumns.Add(col.CloneSubset(indices));
            }
            return new DataFrame(newColumns);
        }

        private class RowIndexComparer : IEqualityComparer<int>
        {
            private readonly IColumn[] _columns;

            public RowIndexComparer(IColumn[] columns)
            {
                _columns = columns;
            }

            public bool Equals(int x, int y)
            {
                for (int c = 0; c < _columns.Length; c++)
                {
                    var col = _columns[c];
                    object? valX = col.GetValue(x);
                    object? valY = col.GetValue(y);
                    if (!object.Equals(valX, valY)) return false;
                }
                return true;
            }

            public int GetHashCode(int obj)
            {
                var hash = new HashCode();
                for (int c = 0; c < _columns.Length; c++)
                {
                    hash.Add(_columns[c].GetValue(obj));
                }
                return hash.ToHashCode();
            }
        }
    }
}