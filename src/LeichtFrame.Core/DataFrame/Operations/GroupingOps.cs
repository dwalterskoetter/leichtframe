using System.Collections.Concurrent;

namespace LeichtFrame.Core
{
    /// <summary>
    /// Provides extension methods for grouping operations on <see cref="DataFrame"/>.
    /// Optimized using generic type dispatching AND parallelism.
    /// </summary>
    public static class DataFrameGroupingExtensions
    {
        private static readonly object NullKey = new object();
        private const int ParallelThreshold = 50_000; // Lower threshold to trigger parallel earlier

        /// <summary>
        /// Groups the rows of the DataFrame by the values in the specified column.
        /// Automatically selects between sequential and parallel execution based on dataset size.
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="columnName">The name of the column to group by.</param>
        /// <returns>A <see cref="GroupedDataFrame"/> object used to apply aggregations.</returns>
        /// <exception cref="ArgumentNullException">Thrown if columnName is null or empty.</exception>
        public static GroupedDataFrame GroupBy(this DataFrame df, string columnName)
        {
            if (string.IsNullOrEmpty(columnName)) throw new ArgumentNullException(nameof(columnName));

            var col = df[columnName];
            Type type = col.DataType;
            Type coreType = Nullable.GetUnderlyingType(type) ?? type;

            if (coreType == typeof(int)) return DispatchGroup<int>(df, columnName);
            if (coreType == typeof(double)) return DispatchGroup<double>(df, columnName);
            if (coreType == typeof(string)) return DispatchGroup<string>(df, columnName);
            if (coreType == typeof(bool)) return DispatchGroup<bool>(df, columnName);
            if (coreType == typeof(DateTime)) return DispatchGroup<DateTime>(df, columnName);

            return GroupBySequential<object>(df, columnName);
        }

        private static GroupedDataFrame DispatchGroup<T>(DataFrame df, string columnName) where T : notnull
        {
            // Strategy Selection
            if (df.RowCount >= ParallelThreshold)
            {
                return GroupByParallel<T>(df, columnName);
            }
            return GroupBySequential<T>(df, columnName);
        }

        // --- Sequential Implementation (Fast for small data) ---
        private static GroupedDataFrame GroupBySequential<T>(DataFrame df, string columnName) where T : notnull
        {
            var col = (IColumn<T>)df[columnName];
            var fastMap = new Dictionary<T, List<int>>();
            var nullIndices = new List<int>();

            for (int i = 0; i < df.RowCount; i++)
            {
                if (col.IsNull(i))
                {
                    nullIndices.Add(i);
                    continue;
                }

                T val = col.GetValue(i);
                if (val == null) // String null check
                {
                    nullIndices.Add(i);
                    continue;
                }

                if (!fastMap.TryGetValue(val, out var indices))
                {
                    indices = new List<int>();
                    fastMap[val] = indices;
                }
                indices.Add(i);
            }

            return CreateResult(df, columnName, fastMap, nullIndices);
        }

        // --- Parallel Implementation (Fast for large data) ---
        private static GroupedDataFrame GroupByParallel<T>(DataFrame df, string columnName) where T : notnull
        {
            var col = (IColumn<T>)df[columnName];

            // Shared state for merge
            var finalMap = new Dictionary<object, List<int>>(1024);
            var globalNulls = new List<int>();
            var mergeLock = new object();

            Parallel.ForEach(
                Partitioner.Create(0, df.RowCount),
                // Local Init: Typed Dictionary + Local Null List
                () => (Map: new Dictionary<T, List<int>>(), Nulls: new List<int>()),

                // Body
                (range, state, local) =>
                {
                    var map = local.Map;
                    var nulls = local.Nulls;

                    for (int i = range.Item1; i < range.Item2; i++)
                    {
                        if (col.IsNull(i))
                        {
                            nulls.Add(i);
                            continue;
                        }

                        T val = col.GetValue(i);
                        if (val == null)
                        {
                            nulls.Add(i);
                            continue;
                        }

                        if (!map.TryGetValue(val, out var indices))
                        {
                            indices = new List<int>();
                            map[val] = indices;
                        }
                        indices.Add(i);
                    }
                    return local;
                },

                // Local Finally (Merge)
                (local) =>
                {
                    lock (mergeLock)
                    {
                        // Merge Nulls
                        if (local.Nulls.Count > 0)
                        {
                            globalNulls.AddRange(local.Nulls);
                        }

                        // Merge Dictionary (Boxing happens here, but only once per group per thread)
                        foreach (var kvp in local.Map)
                        {
                            // Important: Convert Key to Object here
                            object keyObj = kvp.Key;

                            if (finalMap.TryGetValue(keyObj, out var existingList))
                            {
                                existingList.AddRange(kvp.Value);
                            }
                            else
                            {
                                finalMap[keyObj] = kvp.Value;
                            }
                        }
                    }
                }
            );

            // Manual result creation because we populated finalMap directly
            if (globalNulls.Count > 0)
            {
                finalMap[NullKey] = globalNulls;
            }

            return new GroupedDataFrame(df, columnName, finalMap);
        }

        private static GroupedDataFrame CreateResult<T>(
            DataFrame df,
            string columnName,
            Dictionary<T, List<int>> fastMap,
            List<int> nullIndices) where T : notnull
        {
            var finalGroups = new Dictionary<object, List<int>>(fastMap.Count + 1);

            foreach (var kvp in fastMap)
            {
                finalGroups.Add(kvp.Key, kvp.Value);
            }

            if (nullIndices.Count > 0)
            {
                finalGroups.Add(NullKey, nullIndices);
            }

            return new GroupedDataFrame(df, columnName, finalGroups);
        }

        internal static object? GetRealValue(object key)
        {
            return ReferenceEquals(key, NullKey) ? null : key;
        }
    }
}