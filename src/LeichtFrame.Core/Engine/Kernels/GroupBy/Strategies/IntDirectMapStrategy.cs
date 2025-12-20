using System.Buffers;

namespace LeichtFrame.Core.Engine
{
    internal class IntDirectMapStrategy : IGroupByStrategy
    {
        private readonly int _min;
        private readonly int _range;

        public IntDirectMapStrategy(int min, int range)
        {
            _min = min;
            _range = range;
        }

        public GroupedDataFrame Group(DataFrame df, string columnName)
        {
            var col = (IntColumn)df[columnName];
            int rowCount = df.RowCount;
            int bucketCount = _range + 1;
            var values = col.Values.Span;

            // 1. Zählen (Histogramm)
            int[] counts = ArrayPool<int>.Shared.Rent(bucketCount);
            Array.Clear(counts, 0, bucketCount);

            for (int i = 0; i < rowCount; i++)
            {
                // Mapping: Wert -> Array Index (Offset-basiert)
                // TODO: Null Handling bei IntDirectMap ist hier vereinfacht (für Non-Nullable)
                if (!col.IsNull(i))
                {
                    counts[values[i] - _min]++;
                }
            }

            // 2. Offsets berechnen (Prefix Sum)
            int activeGroups = 0;
            for (int i = 0; i < bucketCount; i++)
            {
                if (counts[i] > 0) activeGroups++;
            }

            int[] keys = new int[activeGroups];
            int[] offsets = new int[activeGroups + 1];
            // Hilfsarray: Wo muss der Wert 'i' im Ziel-Index-Array hin?
            int[] mapBucketToGroup = ArrayPool<int>.Shared.Rent(bucketCount);

            int currentOffset = 0;
            int groupIdx = 0;

            for (int i = 0; i < bucketCount; i++)
            {
                int count = counts[i];
                if (count > 0)
                {
                    keys[groupIdx] = i + _min; // Rekonstruktion des echten Wertes
                    offsets[groupIdx] = currentOffset;
                    mapBucketToGroup[i] = groupIdx;

                    // Wir recyceln das 'counts' Array:
                    // Es speichert jetzt die "nächste Schreibposition" für diesen Bucket.
                    counts[i] = currentOffset;

                    currentOffset += count;
                    groupIdx++;
                }
            }
            offsets[activeGroups] = currentOffset; // Sentinel am Ende

            // 3. Schreiben (Scatter)
            int[] indices = new int[rowCount];
            // Nulls separat behandeln falls nötig, hier vereinfacht
            List<int>? nullIndices = null;

            for (int i = 0; i < rowCount; i++)
            {
                if (col.IsNull(i))
                {
                    if (nullIndices == null) nullIndices = new List<int>();
                    nullIndices.Add(i);
                    continue;
                }

                int val = values[i] - _min;
                int dest = counts[val]; // Hole Schreibposition
                indices[dest] = i;
                counts[val]++; // Position inkrementieren
            }

            // Cleanup
            ArrayPool<int>.Shared.Return(counts);
            ArrayPool<int>.Shared.Return(mapBucketToGroup);

            // Wenn wir Nulls hatten, müssen wir das Indices Array vielleicht kürzen, 
            // da 'rowCount' alle Zeilen inkl. Nulls war.
            if (nullIndices != null && nullIndices.Count > 0)
            {
                int validRows = rowCount - nullIndices.Count;
                // Array.Resize wäre hier möglich, oder wir übergeben Slice. 
                // Der Einfachheit halber Resize:
                Array.Resize(ref indices, validRows);
            }

            return new GroupedDataFrame<int>(
                df,
                new[] { columnName },
                keys,
                offsets,
                indices,
                nullIndices?.ToArray()
            );
        }
    }
}