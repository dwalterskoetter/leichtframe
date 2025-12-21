using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace LeichtFrame.Core.Engine.Kernels.GroupBy.Strategies
{
    internal unsafe class MultiColumnHashStrategy : IGroupByStrategy
    {
        public GroupedDataFrame Group(DataFrame df, string ignoredColumnName)
        {
            throw new InvalidOperationException("Use the overload with multiple columns.");
        }

        public GroupedDataFrame Group(DataFrame df, string[] columnNames)
        {
            int rowCount = df.RowCount;
            if (rowCount == 0) return new GroupedDataFrame<int>(df, columnNames, Array.Empty<int>(), new[] { 0 }, Array.Empty<int>(), null);

            var columns = new IColumn[columnNames.Length];
            for (int i = 0; i < columnNames.Length; i++) columns[i] = df[columnNames[i]];

            int* pHashes = (int*)NativeMemory.Alloc((nuint)(rowCount * sizeof(int)));

            int* pMap = null;
            int* pGroupIds = null;

            try
            {
                ComputeCombinedHashes(columns, pHashes, rowCount);

                int capacity = GetNextPowerOfTwo(rowCount * 2);
                if (capacity < 16) capacity = 16;
                int mask = capacity - 1;

                pMap = (int*)NativeMemory.Alloc((nuint)(capacity * sizeof(int)));
                new Span<int>(pMap, capacity).Fill(-1);

                pGroupIds = (int*)NativeMemory.Alloc((nuint)(rowCount * sizeof(int)));

                int groupCount = 0;

                for (int i = 0; i < rowCount; i++)
                {
                    int hash = pHashes[i];
                    int slot = hash & mask;

                    while (true)
                    {
                        int existingRow = pMap[slot];

                        if (existingRow == -1)
                        {
                            pMap[slot] = i;
                            pGroupIds[i] = groupCount++;
                            break;
                        }

                        if (RowsEqual(columns, i, existingRow))
                        {
                            pGroupIds[i] = pGroupIds[existingRow];
                            break;
                        }

                        slot = (slot + 1) & mask;
                    }
                }

                return BuildCsrFromGroupIds(df, columnNames, pGroupIds, rowCount, groupCount);
            }
            finally
            {
                if (pHashes != null) NativeMemory.Free(pHashes);
                if (pMap != null) NativeMemory.Free(pMap);
                if (pGroupIds != null) NativeMemory.Free(pGroupIds);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ComputeCombinedHashes(IColumn[] columns, int* pHashes, int rowCount)
        {
            new Span<int>(pHashes, rowCount).Fill(0);

            foreach (var col in columns)
            {
                const int Multiplier = 397;

                if (col is IntColumn ic)
                {
                    var span = ic.Values.Span;
                    for (int i = 0; i < rowCount; i++)
                    {
                        pHashes[i] = (pHashes[i] * Multiplier) ^ span[i];
                    }
                }
                else if (col is StringColumn sc)
                {
                    for (int i = 0; i < rowCount; i++)
                    {
                        int h = sc.GetHashCodeRaw(i);
                        pHashes[i] = (pHashes[i] * Multiplier) ^ h;
                    }
                }
                else
                {
                    for (int i = 0; i < rowCount; i++)
                    {
                        object? val = col.GetValue(i);
                        int h = val?.GetHashCode() ?? 0;
                        pHashes[i] = (pHashes[i] * Multiplier) ^ h;
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool RowsEqual(IColumn[] columns, int rowA, int rowB)
        {
            foreach (var col in columns)
            {
                if (col is IntColumn ic)
                {
                    if (ic.Get(rowA) != ic.Get(rowB)) return false;
                }
                else if (col is StringColumn sc)
                {
                    if (sc.CompareRaw(rowA, rowB) != 0) return false;
                }
                else
                {
                    if (!object.Equals(col.GetValue(rowA), col.GetValue(rowB))) return false;
                }
            }
            return true;
        }

        private GroupedDataFrame BuildCsrFromGroupIds(DataFrame df, string[] cols, int* pGroupIds, int rowCount, int groupCount)
        {
            int[] offsets = new int[groupCount + 1];
            int[] indices = new int[rowCount];

            int[] counts = new int[groupCount];
            for (int i = 0; i < rowCount; i++) counts[pGroupIds[i]]++;

            int current = 0;
            for (int i = 0; i < groupCount; i++)
            {
                offsets[i] = current;
                current += counts[i];
            }
            offsets[groupCount] = current;

            var writePos = new int[groupCount];
            Array.Copy(offsets, writePos, groupCount);

            for (int i = 0; i < rowCount; i++)
            {
                int gid = pGroupIds[i];
                int pos = writePos[gid]++;
                indices[pos] = i;
            }

            int[] representativeRows = new int[groupCount];
            for (int i = 0; i < groupCount; i++)
            {
                representativeRows[i] = indices[offsets[i]];
            }

            return new GroupedDataFrame<int>(df, cols, representativeRows, offsets, indices, null);
        }

        private int GetNextPowerOfTwo(int x)
        {
            int power = 1;
            while (power < x) power *= 2;
            return power;
        }
    }
}