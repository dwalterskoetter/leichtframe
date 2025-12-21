namespace LeichtFrame.Core.Operations.Sort
{
    /// <summary>
    /// Provides extension methods for sorting DataFrames.
    /// </summary>
    public static class OrderOps
    {
        /// <summary>
        /// Sorts the DataFrame rows in ascending order based on the values in the specified column.
        /// </summary>
        public static DataFrame OrderBy(this DataFrame df, string columnName)
        {
            return SortInternal(df, columnName, ascending: true);
        }

        /// <summary>
        /// Sorts the DataFrame rows in descending order based on the values in the specified column.
        /// </summary>
        public static DataFrame OrderByDescending(this DataFrame df, string columnName)
        {
            return SortInternal(df, columnName, ascending: false);
        }

        // --- NEU: Multi-Column Support ---

        /// <summary>
        /// Sorts the DataFrame by multiple columns.
        /// </summary>
        public static DataFrame OrderBy(this DataFrame df, string[] columnNames, bool[] ascending)
        {
            if (columnNames.Length != ascending.Length)
                throw new ArgumentException("Column names and ascending flags count mismatch");

            var cols = new IColumn[columnNames.Length];
            for (int i = 0; i < columnNames.Length; i++)
            {
                cols[i] = df[columnNames[i]];
            }

            int[] sortedIndices = SortingOps.GetSortedIndices(cols, ascending);

            var newColumns = new List<IColumn>(df.ColumnCount);
            foreach (var col in df.Columns)
            {
                newColumns.Add(col.CloneSubset(sortedIndices));
            }

            return new DataFrame(newColumns);
        }

        // ---

        private static DataFrame SortInternal(DataFrame df, string columnName, bool ascending)
        {
            if (string.IsNullOrEmpty(columnName)) throw new ArgumentNullException(nameof(columnName));

            var sortCol = df[columnName];
            int[] sortedIndices = sortCol.GetSortedIndices(ascending);

            var newColumns = new List<IColumn>(df.ColumnCount);
            foreach (var col in df.Columns)
            {
                newColumns.Add(col.CloneSubset(sortedIndices));
            }

            return new DataFrame(newColumns);
        }
    }
}