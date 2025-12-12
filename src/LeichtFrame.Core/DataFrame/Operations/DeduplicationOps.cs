namespace LeichtFrame.Core
{
    /// <summary>
    /// Provides extension methods for removing duplicate rows.
    /// </summary>
    public static class DeduplicationOps
    {
        /// <summary>
        /// Returns a new DataFrame containing only unique rows.
        /// Checks all columns for equality.
        /// </summary>
        public static DataFrame Distinct(this DataFrame df)
        {
            // Default: Check all columns
            return Distinct(df, df.GetColumnNames().ToArray());
        }

        /// <summary>
        /// Returns a new DataFrame containing rows that are unique based on the specified subset of columns.
        /// Keeps the first occurrence of each duplicate.
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="columnNames">The columns to check for uniqueness.</param>
        public static DataFrame Distinct(this DataFrame df, params string[] columnNames)
        {
            if (columnNames == null || columnNames.Length == 0)
                throw new ArgumentException("At least one column must be specified.");

            // 1. Identify columns to check
            var colsToCheck = new IColumn[columnNames.Length];
            for (int i = 0; i < columnNames.Length; i++)
            {
                colsToCheck[i] = df[columnNames[i]];
            }

            // 2. Setup HashSet with custom Comparer
            // This comparer treats an 'int' (rowIndex) as the row content.
            var comparer = new RowIndexComparer(colsToCheck);
            var seenRows = new HashSet<int>(comparer);
            var uniqueIndices = new List<int>(df.RowCount);

            // 3. Scan
            for (int i = 0; i < df.RowCount; i++)
            {
                // HashSet.Add returns true if the element was added (was new)
                if (seenRows.Add(i))
                {
                    uniqueIndices.Add(i);
                }
            }

            // 4. Create Result (Subset)
            // If everything is unique (Count equal), return original? 
            // Better strict: return clone/subset to ensure consistent behavior unless immutable.
            if (uniqueIndices.Count == df.RowCount) return df; // Optimization

            var newColumns = new List<IColumn>(df.ColumnCount);
            foreach (var col in df.Columns)
            {
                newColumns.Add(col.CloneSubset(uniqueIndices));
            }

            return new DataFrame(newColumns);
        }

        /// <summary>
        /// Specialized Comparer that compares two rows by their index.
        /// This avoids allocating objects for keys.
        /// </summary>
        private class RowIndexComparer : IEqualityComparer<int>
        {
            private readonly IColumn[] _columns;

            public RowIndexComparer(IColumn[] columns)
            {
                _columns = columns;
            }

            public bool Equals(int x, int y)
            {
                // Compare every column value at index x and y
                for (int c = 0; c < _columns.Length; c++)
                {
                    var col = _columns[c];
                    // We use the object-based GetValue for simplicity in MVP.
                    // Optimizations (typed switch) could be done later if this is hot path.
                    object? valX = col.GetValue(x);
                    object? valY = col.GetValue(y);

                    if (!object.Equals(valX, valY)) return false;
                }
                return true;
            }

            public int GetHashCode(int obj)
            {
                // Combine hash codes of all column values for row 'obj'
                var hash = new HashCode();
                for (int c = 0; c < _columns.Length; c++)
                {
                    object? val = _columns[c].GetValue(obj);
                    hash.Add(val);
                }
                return hash.ToHashCode();
            }
        }
    }
}