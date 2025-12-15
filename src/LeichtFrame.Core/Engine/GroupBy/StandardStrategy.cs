using System.Collections.Concurrent;
using LeichtFrame.Core.Internal;

namespace LeichtFrame.Core
{
    /// <summary>
    /// The existing implementation: Safe Managed Code.
    /// Uses PrimitiveKeyMap (Dictionary-based) and Managed Parallelism for Strings.
    /// </summary>
    internal class StandardStrategy : IGroupByStrategy
    {
        private const int ParallelThreshold = 100_000;
        private const int BufferSize = 512;

        public GroupedDataFrame Group(DataFrame df, string columnName)
        {
            var col = df[columnName];
            Type t = Nullable.GetUnderlyingType(col.DataType) ?? col.DataType;

            // Primitives -> Sequential
            if (t == typeof(int)) return GroupByPrimitiveSequential<int>(df, columnName);
            if (t == typeof(double)) return GroupByPrimitiveSequential<double>(df, columnName);
            if (t == typeof(long)) return GroupByPrimitiveSequential<long>(df, columnName);
            if (t == typeof(bool)) return GroupByPrimitiveSequential<bool>(df, columnName);
            if (t == typeof(DateTime)) return GroupByPrimitiveSequential<DateTime>(df, columnName);

            // Strings -> Smart Dispatch
            if (t == typeof(string)) return GroupByString(df, columnName);

            throw new NotSupportedException($"GroupBy not implemented for type {t.Name}");
        }

        private GroupedDataFrame GroupByPrimitiveSequential<T>(DataFrame df, string columnName) where T : unmanaged, IEquatable<T>
        {
            var col = (IColumn<T>)df[columnName];
            var map = new PrimitiveKeyMap<T>(Math.Max(128, df.RowCount / 10), df.RowCount);
            var nullIndices = new List<int>();
            for (int i = 0; i < df.RowCount; i++)
            {
                if (col.IsNull(i)) { nullIndices.Add(i); continue; }
                map.AddRow(col.GetValue(i), i);
            }
            var csr = map.ToCSR();
            map.Dispose();
            return new GroupedDataFrame<T>(df, columnName, csr.Keys, csr.GroupOffsets, csr.RowIndices, nullIndices.Count > 0 ? nullIndices.ToArray() : null);
        }

        private GroupedDataFrame GroupByString(DataFrame df, string columnName)
        {
            var col = (StringColumn)df[columnName];

            if (df.RowCount < ParallelThreshold)
            {
                return GroupByStringSequential(df, columnName);
            }

            if (ShouldUseParallelStringProcessing(col))
            {
                return GroupByStringParallel(df, columnName);
            }

            return GroupByStringSequential(df, columnName);
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

        private GroupedDataFrame GroupByStringSequential(DataFrame df, string columnName)
        {
            var col = (StringColumn)df[columnName];
            var map = new StringKeyMap(col.RawBytes, col.Offsets, Math.Max(128, df.RowCount / 100), df.RowCount);
            var nullIndices = new List<int>();

            for (int i = 0; i < df.RowCount; i++)
            {
                if (col.IsNull(i)) { nullIndices.Add(i); continue; }
                map.AddRow(i);
            }

            var csr = map.ToCSR();
            map.Dispose();
            return new GroupedDataFrame<string>(df, columnName, csr.Keys, csr.Offsets, csr.Indices, nullIndices.Count > 0 ? nullIndices.ToArray() : null);
        }

        private GroupedDataFrame GroupByStringParallel(DataFrame df, string columnName)
        {
            var col = (StringColumn)df[columnName];
            int rowCount = df.RowCount;
            int partitionCount = Math.Max(16, Environment.ProcessorCount * 2);
            byte[] rawBytes = col.RawBytes;
            int[] offsets = col.Offsets;

            var globalBuckets = new List<int>[partitionCount];
            for (int i = 0; i < partitionCount; i++) globalBuckets[i] = new List<int>(rowCount / partitionCount);
            var globalLocks = new object[partitionCount];
            for (int i = 0; i < partitionCount; i++) globalLocks[i] = new object();
            var nullIndices = new ConcurrentBag<int>();

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
                    int len = offsets[i + 1] - start;
                    int hash = -2128831035;
                    int end = start + len;
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
                            int count = counts[p];
                            for (int k = 0; k < count; k++) gBucket.Add(buffer[k]);
                        }
                    }
                }
            });

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
                for (int j = 0; j < pIndices.Length; j++) finalIndices[indexOffset + j] = currentBucket[pIndices[j]];
                for (int i = 0; i < pGroupCount; i++)
                {
                    int groupSize = pOffsets[i + 1] - pOffsets[i];
                    currentOffsetValue += groupSize;
                    finalOffsets[keyOffset + i + 1] = currentOffsetValue;
                }
                keyOffset += pGroupCount;
                indexOffset += pIndices.Length;
            }
            return new GroupedDataFrame<string>(df, columnName, finalKeys, finalOffsets, finalIndices, !nullIndices.IsEmpty ? nullIndices.ToArray() : null);
        }
    }
}