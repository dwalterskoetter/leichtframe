namespace LeichtFrame.Core
{
    /// <summary>
    /// Provides extension methods for sorting DataFrames.
    /// </summary>
    public static class OrderOps
    {
        /// <summary>
        /// Sorts the DataFrame rows in ascending order based on the values in the specified column.
        /// Returns a new DataFrame with reordered rows.
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="columnName">The name of the column to sort by.</param>
        /// <returns>A new, sorted DataFrame.</returns>
        public static DataFrame OrderBy(this DataFrame df, string columnName)
        {
            return SortInternal(df, columnName, ascending: true);
        }

        /// <summary>
        /// Sorts the DataFrame rows in descending order based on the values in the specified column.
        /// Returns a new DataFrame with reordered rows.
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="columnName">The name of the column to sort by.</param>
        /// <returns>A new, sorted DataFrame.</returns>
        public static DataFrame OrderByDescending(this DataFrame df, string columnName)
        {
            return SortInternal(df, columnName, ascending: false);
        }

        private static DataFrame SortInternal(DataFrame df, string columnName, bool ascending)
        {
            if (string.IsNullOrEmpty(columnName)) throw new ArgumentNullException(nameof(columnName));

            // 1. Get the column to sort by
            var sortCol = df[columnName];

            // 2. Calculate the permutation vector (ArgSort)
            // This gives us the indices [2, 0, 1, ...] representing the sorted order.
            int[] sortedIndices = sortCol.GetSortedIndices(ascending);

            // 3. Reorder all columns based on these indices
            // We use CloneSubset, which creates a deep copy of the data in the new order.
            var newColumns = new List<IColumn>(df.ColumnCount);

            foreach (var col in df.Columns)
            {
                newColumns.Add(col.CloneSubset(sortedIndices));
            }

            return new DataFrame(newColumns);
        }
    }
}