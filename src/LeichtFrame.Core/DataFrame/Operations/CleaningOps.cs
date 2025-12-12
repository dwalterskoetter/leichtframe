namespace LeichtFrame.Core
{
    /// <summary>
    /// Provides extension methods for cleaning data (handling nulls).
    /// </summary>
    public static class CleaningOps
    {
        /// <summary>
        /// Removes all rows that contain at least one null value in any column.
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <returns>A new DataFrame with only complete rows.</returns>
        public static DataFrame DropNulls(this DataFrame df)
        {
            // 1. Identify columns that are actually nullable (Optimization)
            var nullableCols = new List<IColumn>();
            foreach (var col in df.Columns)
            {
                if (col.IsNullable) nullableCols.Add(col);
            }

            // If no columns are nullable, return the original (or a clone if we want strict immutability semantics? 
            // Usually DropNulls implies "if nothing dropped, return self" is acceptable for performance).
            if (nullableCols.Count == 0) return df; // Zero-Copy optimization

            var indices = new List<int>(df.RowCount);

            // 2. Scan rows
            for (int i = 0; i < df.RowCount; i++)
            {
                bool hasNull = false;
                // Only check nullable columns
                for (int c = 0; c < nullableCols.Count; c++)
                {
                    if (nullableCols[c].IsNull(i))
                    {
                        hasNull = true;
                        break;
                    }
                }

                if (!hasNull)
                {
                    indices.Add(i);
                }
            }

            // 3. Create Subset
            // If we kept all rows, return original
            if (indices.Count == df.RowCount) return df;

            var newColumns = new List<IColumn>(df.ColumnCount);
            foreach (var col in df.Columns)
            {
                newColumns.Add(col.CloneSubset(indices));
            }

            return new DataFrame(newColumns);
        }

        /// <summary>
        /// Replaces null values in the specified column with a constant value.
        /// </summary>
        /// <typeparam name="T">The type of the column data.</typeparam>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="columnName">The name of the column to fill.</param>
        /// <param name="value">The value to replace nulls with.</param>
        /// <returns>A new DataFrame with the filled column.</returns>
        public static DataFrame FillNull<T>(this DataFrame df, string columnName, T value)
        {
            var targetCol = df[columnName];

            // If column is not nullable, nothing to do (return self logic, but we need to reconstruct DF to be safe?)
            // Let's assume we modify the specific column in the new DF.
            if (!targetCol.IsNullable) return df;

            // 1. Create a deep copy of the target column but WITHOUT NullBitmap support (IsNullable = false)
            // Strategy: We create a new column, copy all values manually, replacing nulls on the fly.

            // Note: We cannot easily "Deep Copy" an IColumn via Interface.
            // We use ColumnFactory to create a fresh one.
            var newCol = ColumnFactory.Create<T>(columnName, df.RowCount, isNullable: false);

            if (targetCol is IColumn<T> typedSource)
            {
                for (int i = 0; i < df.RowCount; i++)
                {
                    if (typedSource.IsNull(i))
                    {
                        newCol.Append(value);
                    }
                    else
                    {
                        newCol.Append(typedSource.GetValue(i));
                    }
                }
            }
            else
            {
                throw new ArgumentException($"Column '{columnName}' is not of type {typeof(T).Name}");
            }

            // 2. Build new DataFrame
            var newColumns = new List<IColumn>(df.ColumnCount);
            foreach (var col in df.Columns)
            {
                if (col.Name == columnName)
                {
                    newColumns.Add(newCol);
                }
                else
                {
                    // Zero-Copy for other columns
                    newColumns.Add(col);
                }
            }

            return new DataFrame(newColumns);
        }
    }
}