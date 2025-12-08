namespace LeichtFrame.Core
{
    /// <summary>
    /// Provides extension methods for grouping operations on <see cref="DataFrame"/>.
    /// </summary>
    public static class DataFrameGroupingExtensions
    {
        // Sentinel object to represent 'null' in the dictionary key
        private static readonly object NullKey = new object();

        /// <summary>
        /// Groups the rows of the DataFrame by the values in the specified column.
        /// Uses a Hash-Map approach (O(n)).
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="columnName">The name of the column to group by.</param>
        /// <returns>A <see cref="GroupedDataFrame"/> object used to apply aggregations.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="columnName"/> is null or empty.</exception>
        public static GroupedDataFrame GroupBy(this DataFrame df, string columnName)
        {
            if (string.IsNullOrEmpty(columnName)) throw new ArgumentNullException(nameof(columnName));

            var col = df[columnName];
            var groups = new Dictionary<object, List<int>>();

            // We iterate over all rows (O(n))
            for (int i = 0; i < df.RowCount; i++)
            {
                // 1. Get value (untyped is okay here for MVP, as GroupBy key is usually object-like)
                // Optimization idea for later: Generic implementation for IntColumn to avoid boxing.
                object? val = col.GetValue(i);

                // 2. Null-Handling for Dictionary Key
                object key = val ?? NullKey;

                // 3. Insert into bucket
                // (net8.0 allows CollectionsMarshal for high-perf, but here standard way for readability)
                if (!groups.TryGetValue(key, out var indices))
                {
                    indices = new List<int>();
                    groups[key] = indices;
                }

                indices.Add(i);
            }

            return new GroupedDataFrame(df, columnName, groups);
        }

        /// <summary>
        /// Helper to retrieve the original value from a dictionary key (converts NullKey back to null).
        /// </summary>
        internal static object? GetRealValue(object key)
        {
            return ReferenceEquals(key, NullKey) ? null : key;
        }
    }
}