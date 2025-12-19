namespace LeichtFrame.Core
{
    /// <summary>
    /// Provides extension methods for selecting columns and slicing rows from a <see cref="DataFrame"/>.
    /// </summary>
    public static class DataFrameSelectionExtensions
    {
        /// <summary>
        /// Projects the DataFrame to a new DataFrame containing only the selected columns.
        /// This is a Zero-Copy operation: The new DataFrame shares the underlying column data with the original.
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="columnNames">The names of the columns to select.</param>
        /// <returns>A new DataFrame instance containing the selected columns.</returns>
        public static DataFrame Select(this DataFrame df, params string[] columnNames)
        {
            if (df == null) throw new ArgumentNullException(nameof(df));
            if (columnNames == null || columnNames.Length == 0)
                throw new ArgumentException("At least one column must be selected.", nameof(columnNames));

            // We collect the column references from the original.
            var selectedColumns = new List<IColumn>(columnNames.Length);

            foreach (var name in columnNames)
            {
                selectedColumns.Add(df[name]);
            }

            // Create a new container with the same column instances.
            return new DataFrame(selectedColumns);
        }

        /// <summary>
        /// Returns a zero-copy view of the DataFrame restricted to the specified row range.
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="start">The zero-based starting row index.</param>
        /// <param name="length">The number of rows to include in the slice.</param>
        /// <returns>A new DataFrame containing the subset of rows.</returns>
        public static DataFrame Slice(this DataFrame df, int start, int length)
        {
            if (start < 0) throw new ArgumentOutOfRangeException(nameof(start));

            // Bounds adjusting (Robustness)
            if (start >= df.RowCount)
            {
                // Return empty DataFrame with same schema
                return DataFrame.Create(df.Schema, 0);
            }

            int validLength = Math.Min(length, df.RowCount - start);

            var newColumns = new List<IColumn>(df.ColumnCount);

            foreach (var col in df.Columns)
            {
                // Magic: Create SlicedColumn<T> dynamically
                var genericType = typeof(SlicedColumn<>).MakeGenericType(col.DataType);

                // Invoke Constructor: SlicedColumn(source, offset, length)
                var slicedCol = Activator.CreateInstance(genericType, col, start, validLength);

                newColumns.Add((IColumn)slicedCol!);
            }

            return new DataFrame(newColumns);
        }

        /// <summary>
        /// Returns the first <paramref name="count"/> rows of the DataFrame.
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="count">The number of rows to return from the beginning.</param>
        /// <returns>A new DataFrame containing the first rows.</returns>
        public static DataFrame Head(this DataFrame df, int count)
        {
            return df.Slice(0, count);
        }

        /// <summary>
        /// Returns the last <paramref name="count"/> rows of the DataFrame.
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="count">The number of rows to return from the end.</param>
        /// <returns>A new DataFrame containing the last rows.</returns>
        public static DataFrame Tail(this DataFrame df, int count)
        {
            int start = Math.Max(0, df.RowCount - count);
            // length is count, but Slice logic handles if start+count > RowCount (though here it matches)
            int length = Math.Min(count, df.RowCount);
            return df.Slice(start, length);
        }
    }
}