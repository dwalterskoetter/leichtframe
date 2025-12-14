using System.Reflection;

namespace LeichtFrame.Core
{
    /// <summary>
    /// Provides extension methods to convert standard IEnumerable collections into LeichtFrame DataFrames.
    /// Supports batched processing to handle large datasets efficiently.
    /// </summary>
    public static class EnumerableDataFrameExtensions
    {
        /// <summary>
        /// Streams an IEnumerable of objects into multiple DataFrames (batches).
        /// This allows processing large collections without holding all objects in memory at once.
        /// </summary>
        /// <typeparam name="T">The type of the objects (POCO).</typeparam>
        /// <param name="source">The source collection.</param>
        /// <param name="batchSize">The number of rows per batch.</param>
        /// <returns>An enumerable of DataFrames.</returns>
        public static IEnumerable<DataFrame> ToDataFrameBatches<T>(this IEnumerable<T> source, int batchSize)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (batchSize <= 0) throw new ArgumentOutOfRangeException(nameof(batchSize));

            // 1. One-time Setup: Infer Schema and Cache Reflection Data
            // We do this ONCE per stream, not per batch.
            var schema = DataFrameSchema.FromType<T>();
            var type = typeof(T);

            // Align PropertyInfos with Column Indices for fast access
            int colCount = schema.Columns.Count;
            var propertyCache = new PropertyInfo[colCount];

            for (int i = 0; i < colCount; i++)
            {
                string name = schema.Columns[i].Name;
                var prop = type.GetProperty(name);
                if (prop == null) throw new InvalidOperationException($"Property '{name}' not found on type '{type.Name}' during mapping.");
                propertyCache[i] = prop;
            }

            // 2. Iteration & Buffering
            var buffer = new List<T>(batchSize);

            foreach (var item in source)
            {
                buffer.Add(item);

                if (buffer.Count >= batchSize)
                {
                    yield return FlushBatch(buffer, schema, propertyCache);
                    buffer.Clear();
                }
            }

            // 3. Flush Remainder
            if (buffer.Count > 0)
            {
                yield return FlushBatch(buffer, schema, propertyCache);
            }
        }

        private static DataFrame FlushBatch<T>(List<T> buffer, DataFrameSchema schema, PropertyInfo[] propertyCache)
        {
            // Create DataFrame with exact capacity
            var df = DataFrame.Create(schema, buffer.Count);
            int colCount = df.ColumnCount;

            // Cache IColumn references to avoid indexer lookups in the loop
            var columns = new IColumn[colCount];
            for (int i = 0; i < colCount; i++)
            {
                columns[i] = df.Columns[i];
            }

            // Fill Data
            foreach (var item in buffer)
            {
                for (int c = 0; c < colCount; c++)
                {
                    object? val = propertyCache[c].GetValue(item);
                    columns[c].AppendObject(val);
                }
            }

            return df;
        }
    }
}