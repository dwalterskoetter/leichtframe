namespace LeichtFrame.Core
{
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
    }
}