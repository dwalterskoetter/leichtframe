using System.Collections.Concurrent;

namespace LeichtFrame.Core.Engine
{
    internal class StringSmartStrategy : IGroupByStrategy
    {
        private const int ParallelThreshold = 100_000;
        private const int BufferSize = 512;

        public GroupedDataFrame Group(DataFrame df, string columnName)
        {
            var col = (StringColumn)df[columnName];

            // 1. Zu wenig Daten -> Sequenziell ist schneller (kein Overhead)
            if (df.RowCount < ParallelThreshold)
            {
                return GroupSequential(df, columnName, col);
            }

            // 2. Sampling: Prüfe Kardinalität
            if (ShouldUseParallelStringProcessing(col))
            {
                return GroupParallel(df, columnName, col);
            }

            return GroupSequential(df, columnName, col);
        }

        private bool ShouldUseParallelStringProcessing(StringColumn col)
        {
            int sampleSize = Math.Min(col.Length, 100);
            if (sampleSize == 0) return false;

            var uniqueSampler = new HashSet<string>(sampleSize);
            int uniqueCount = 0;

            for (int i = 0; i < sampleSize; i += 1)
            {
                string? val = col.Get(i);
                if (val != null && uniqueSampler.Add(val)) uniqueCount++;
                if (uniqueCount > 20) return true;
            }
            return false;
        }

        private GroupedDataFrame GroupSequential(DataFrame df, string colName, StringColumn col)
        {
            var map = new StringKeyMap(col.RawBytes, col.Offsets, Math.Max(128, df.RowCount / 100), df.RowCount);
            var nullIndices = new List<int>();

            for (int i = 0; i < df.RowCount; i++)
            {
                if (col.IsNull(i)) { nullIndices.Add(i); continue; }
                map.AddRow(i);
            }

            var csr = map.ToCSR();
            map.Dispose();

            // FIX: new[] { colName } statt colName
            return new GroupedDataFrame<string>(
                df, new[] { colName }, csr.Keys, csr.Offsets, csr.Indices,
                nullIndices.Count > 0 ? nullIndices.ToArray() : null
            );
        }

        private GroupedDataFrame GroupParallel(DataFrame df, string colName, StringColumn col)
        {
            int rowCount = df.RowCount;
            int partitionCount = Math.Max(16, Environment.ProcessorCount * 2);
            byte[] rawBytes = col.RawBytes;
            int[] offsets = col.Offsets;

            var globalBuckets = new List<int>[partitionCount];
            for (int i = 0; i < partitionCount; i++) globalBuckets[i] = new List<int>(rowCount / partitionCount);
            var globalLocks = new object[partitionCount];
            for (int i = 0; i < partitionCount; i++) globalLocks[i] = new object();
            var nullIndices = new ConcurrentBag<int>();

            // Phase 1: Partition Rows (Scatter)
            Parallel.ForEach(Partitioner.Create(0, rowCount), () =>
            {
                var buffers = new int[partitionCount][];
                for (int i = 0; i < partitionCount; i++) buffers[i] = new int[BufferSize];
                var counts = new int[partitionCount];
                return (buffers, counts);
            },
            (range, state, localState) =>
            {
                var (buffers, counts) = localState;
                for (int i = range.Item1; i < range.Item2; i++)
                {
                    if (col.IsNull(i)) { nullIndices.Add(i); continue; }

                    int start = offsets[i];
                    int end = offsets[i + 1];
                    int hash = -2128831035;
                    for (int k = start; k < end; k++) hash = (hash ^ rawBytes[k]) * 16777619;

                    int pIdx = (hash & 0x7FFFFFFF) % partitionCount;

                    buffers[pIdx][counts[pIdx]] = i;
                    counts[pIdx]++;

                    if (counts[pIdx] == BufferSize)
                    {
                        lock (globalLocks[pIdx])
                        {
                            var gBucket = globalBuckets[pIdx];
                            var buffer = buffers[pIdx];
                            for (int k = 0; k < BufferSize; k++) gBucket.Add(buffer[k]);
                        }
                        counts[pIdx] = 0;
                    }
                }
                return localState;
            },
            (localState) =>
            {
                var (buffers, counts) = localState;
                for (int p = 0; p < partitionCount; p++)
                {
                    if (counts[p] > 0)
                    {
                        lock (globalLocks[p])
                        {
                            var gBucket = globalBuckets[p];
                            var buffer = buffers[p];
                            for (int k = 0; k < counts[p]; k++) gBucket.Add(buffer[k]);
                        }
                    }
                }
            });

            // Phase 2: Process Partitions independently
            var partialResults = new (string[] Keys, int[] Offsets, int[] Indices)[partitionCount];

            Parallel.For(0, partitionCount, p =>
            {
                var indices = globalBuckets[p];
                if (indices.Count == 0)
                {
                    partialResults[p] = (Array.Empty<string>(), new int[] { 0 }, Array.Empty<int>());
                    return;
                }
                var map = new StringKeyMap(rawBytes, offsets, indices.Count, indices.Count);
                for (int k = 0; k < indices.Count; k++) map.AddRow(indices[k], k);
                partialResults[p] = map.ToCSR();
                map.Dispose();
            });

            // Phase 3: Merge Results
            int totalGroups = 0;
            int totalIndices = 0;
            foreach (var pr in partialResults) { totalGroups += pr.Keys.Length; totalIndices += pr.Indices.Length; }

            var finalKeys = new string[totalGroups];
            var finalOffsets = new int[totalGroups + 1];
            var finalIndices = new int[totalIndices];
            int keyOffset = 0; int indexOffset = 0; int currentOffsetValue = 0;
            finalOffsets[0] = 0;

            for (int p = 0; p < partitionCount; p++)
            {
                var (pKeys, pOffsets, pIndices) = partialResults[p];
                int pGroupCount = pKeys.Length;
                if (pGroupCount == 0) continue;

                Array.Copy(pKeys, 0, finalKeys, keyOffset, pGroupCount);
                var currentBucket = globalBuckets[p];

                for (int j = 0; j < pIndices.Length; j++)
                    finalIndices[indexOffset + j] = currentBucket[pIndices[j]];

                for (int i = 0; i < pGroupCount; i++)
                {
                    int groupSize = pOffsets[i + 1] - pOffsets[i];
                    currentOffsetValue += groupSize;
                    finalOffsets[keyOffset + i + 1] = currentOffsetValue;
                }
                keyOffset += pGroupCount;
                indexOffset += pIndices.Length;
            }

            // FIX: new[] { colName } statt colName
            return new GroupedDataFrame<string>(
                df, new[] { colName }, finalKeys, finalOffsets, finalIndices,
                !nullIndices.IsEmpty ? nullIndices.ToArray() : null
            );
        }
    }
}