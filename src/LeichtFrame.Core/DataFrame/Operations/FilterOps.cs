namespace LeichtFrame.Core
{
    /// <summary>
    /// Provides extension methods for filtering <see cref="DataFrame"/> rows based on predicates.
    /// </summary>
    public static class DataFrameFilterExtensions
    {
        /// <summary>
        /// Filters rows based on a predicate function.
        /// Creates a new <see cref="DataFrame"/> with COPIED data containing only the matching rows.
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="predicate">A function to test each row. Return <c>true</c> to keep the row, <c>false</c> to drop it.</param>
        /// <returns>A new DataFrame containing only the rows that satisfy the condition.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="predicate"/> is null.</exception>
        public static DataFrame Where(this DataFrame df, Func<RowView, bool> predicate)
        {
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));

            // 1. Phase: Scan (Collect indices)
            // We use a capacity estimate to minimize resizing of the list
            var indices = new List<int>(df.RowCount / 2);

            // RowView is a struct (stack-only), so very cheap to create
            for (int i = 0; i < df.RowCount; i++)
            {
                var row = new RowView(i, df.Columns, df.Schema);
                if (predicate(row))
                {
                    indices.Add(i);
                }
            }

            // 2. Phase: Copy (Column-wise)
            var newColumns = new List<IColumn>(df.ColumnCount);
            foreach (var col in df.Columns)
            {
                // Each column takes care of efficiently copying the indices
                newColumns.Add(col.CloneSubset(indices));
            }

            return new DataFrame(newColumns);
        }
    }
}