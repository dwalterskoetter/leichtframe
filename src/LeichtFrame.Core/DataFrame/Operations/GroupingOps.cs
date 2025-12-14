using System.Collections.Concurrent;

namespace LeichtFrame.Core
{
    /// <summary>
    /// Provides extension methods for grouping operations on <see cref="DataFrame"/>.
    /// Automatically selects the best execution strategy (Sequential vs. Parallel) based on dataset size.
    /// </summary>
    public static class DataFrameGroupingExtensions
    {
        // Sentinel object to represent 'null' in the dictionary key
        private static readonly object NullKey = new object();

        // Threshold to switch to parallel processing.
        // Parallel overhead (allocation/merging) is usually only worth it for > 50k-100k rows.
        private const int ParallelThreshold = 100_000;

        /// <summary>
        /// Groups the rows of the DataFrame by the values in the specified column.
        /// Automatically utilizes parallel processing for large datasets.
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="columnName">The name of the column to group by.</param>
        /// <returns>A <see cref="GroupedDataFrame"/> object used to apply aggregations.</returns>
        public static GroupedDataFrame GroupBy(this DataFrame df, string columnName)
        {
            if (string.IsNullOrEmpty(columnName)) throw new ArgumentNullException(nameof(columnName));

            // Adaptive Strategy Selection
            if (df.RowCount >= ParallelThreshold)
            {
                return GroupByParallel(df, columnName);
            }

            return GroupBySequential(df, columnName);
        }

        private static GroupedDataFrame GroupBySequential(DataFrame df, string columnName)
        {
            var col = df[columnName];
            var groups = new Dictionary<object, List<int>>();

            for (int i = 0; i < df.RowCount; i++)
            {
                object? val = col.GetValue(i);
                object key = val ?? NullKey;

                if (!groups.TryGetValue(key, out var indices))
                {
                    indices = new List<int>();
                    groups[key] = indices;
                }

                indices.Add(i);
            }

            return new GroupedDataFrame(df, columnName, groups);
        }

        private static GroupedDataFrame GroupByParallel(DataFrame df, string columnName)
        {
            var col = df[columnName];

            // Final result container
            var finalGroups = new Dictionary<object, List<int>>();
            var mergeLock = new object();

            // Map-Reduce:
            // 1. Partition range
            // 2. Build local dictionaries (No Lock)
            // 3. Merge into final dictionary (Lock)
            Parallel.ForEach(
                Partitioner.Create(0, df.RowCount),
                () => new Dictionary<object, List<int>>(), // Local Init
                (range, state, localDict) =>
                {
                    // Hot Loop
                    for (int i = range.Item1; i < range.Item2; i++)
                    {
                        object? val = col.GetValue(i);
                        object key = val ?? NullKey;

                        if (!localDict.TryGetValue(key, out var indices))
                        {
                            indices = new List<int>();
                            localDict[key] = indices;
                        }
                        indices.Add(i);
                    }
                    return localDict;
                },
                (localDict) =>
                {
                    // Merge Step
                    lock (mergeLock)
                    {
                        foreach (var kvp in localDict)
                        {
                            if (finalGroups.TryGetValue(kvp.Key, out var mainIndices))
                            {
                                mainIndices.AddRange(kvp.Value);
                            }
                            else
                            {
                                finalGroups[kvp.Key] = kvp.Value;
                            }
                        }
                    }
                }
            );

            return new GroupedDataFrame(df, columnName, finalGroups);
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