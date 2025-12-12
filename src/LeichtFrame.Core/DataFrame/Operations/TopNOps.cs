namespace LeichtFrame.Core
{
    /// <summary>
    /// Provides optimized Top-N / Bottom-N selection algorithms (Heap-based).
    /// </summary>
    public static class TopNOps
    {
        /// <summary>
        /// Returns the N rows with the smallest values in the specified column.
        /// (Equivalent to OrderBy(column).Head(n), but significantly faster).
        /// </summary>
        public static DataFrame Smallest(this DataFrame df, int n, string columnName)
        {
            return TopNInternal(df, n, columnName, ascending: true);
        }

        /// <summary>
        /// Returns the N rows with the largest values in the specified column.
        /// (Equivalent to OrderByDescending(column).Head(n), but significantly faster).
        /// </summary>
        public static DataFrame Largest(this DataFrame df, int n, string columnName)
        {
            return TopNInternal(df, n, columnName, ascending: false);
        }

        private static DataFrame TopNInternal(DataFrame df, int n, string columnName, bool ascending)
        {
            if (n <= 0) return df.Head(0); // Return empty schema
            if (n >= df.RowCount) return ascending ? df.OrderBy(columnName) : df.OrderByDescending(columnName);

            var col = df[columnName];
            var indices = new int[n];
            int finalCount = 0;

            if (col is IntColumn ic)
            {
                finalCount = GetIndices(ic, n, ascending, indices);
            }
            else if (col is DoubleColumn dc)
            {
                finalCount = GetIndices(dc, n, ascending, indices);
            }
            else if (col is StringColumn sc)
            {
                finalCount = GetIndices(sc, n, ascending, indices);
            }
            else
            {
                // Fallback: Full Sort
                return ascending ? df.OrderBy(columnName).Head(n) : df.OrderByDescending(columnName).Head(n);
            }

            Array.Resize(ref indices, finalCount);

            // Final Sort of the small result set
            var subsetIndices = SortIndicesByColumn(indices, col, ascending);

            var newColumns = new List<IColumn>(df.ColumnCount);
            foreach (var c in df.Columns)
            {
                newColumns.Add(c.CloneSubset(subsetIndices));
            }
            return new DataFrame(newColumns);
        }

        // --- Type Specific Implementations ---

        private static int GetIndices(IntColumn col, int n, bool smallestN, int[] resultBuffer)
        {
            var comparer = smallestN
                ? Comparer<int>.Create((x, y) => y.CompareTo(x))
                : Comparer<int>.Default;

            var queue = new PriorityQueue<int, int>(n, comparer);

            for (int i = 0; i < col.Length; i++)
            {
                int val = col.Get(i);

                if (queue.Count < n)
                {
                    queue.Enqueue(i, val);
                }
                else
                {
                    if (queue.TryPeek(out int _, out int worstVal))
                    {
                        bool shouldSwap = smallestN
                            ? val < worstVal
                            : val > worstVal;

                        if (shouldSwap)
                        {
                            queue.Dequeue();
                            queue.Enqueue(i, val);
                        }
                    }
                }
            }

            int count = queue.Count;
            for (int i = count - 1; i >= 0; i--)
            {
                resultBuffer[i] = queue.Dequeue();
            }
            return count;
        }

        private static int GetIndices(DoubleColumn col, int n, bool smallestN, int[] resultBuffer)
        {
            var comparer = smallestN
                ? Comparer<double>.Create((x, y) => y.CompareTo(x))
                : Comparer<double>.Default;

            var queue = new PriorityQueue<int, double>(n, comparer);

            for (int i = 0; i < col.Length; i++)
            {
                if (col.IsNull(i)) continue;

                double val = col.Get(i);

                if (queue.Count < n)
                {
                    queue.Enqueue(i, val);
                }
                else
                {
                    if (queue.TryPeek(out int _, out double worstVal))
                    {
                        bool shouldSwap = smallestN ? val < worstVal : val > worstVal;
                        if (shouldSwap)
                        {
                            queue.Dequeue();
                            queue.Enqueue(i, val);
                        }
                    }
                }
            }

            int count = queue.Count;
            for (int i = count - 1; i >= 0; i--) resultBuffer[i] = queue.Dequeue();
            return count;
        }

        private static int GetIndices(StringColumn col, int n, bool smallestN, int[] resultBuffer)
        {
            IComparer<string> comparer = smallestN
                ? (IComparer<string>)Comparer<string>.Create((x, y) => string.CompareOrdinal(y, x))
                : StringComparer.Ordinal;

            var queue = new PriorityQueue<int, string>(n, comparer);

            for (int i = 0; i < col.Length; i++)
            {
                if (col.IsNull(i)) continue;
                string val = col.Get(i)!;

                if (queue.Count < n)
                {
                    queue.Enqueue(i, val);
                }
                else
                {
                    if (queue.TryPeek(out int _, out string? worstVal))
                    {
                        bool shouldSwap = smallestN
                            ? string.CompareOrdinal(val, worstVal) < 0
                            : string.CompareOrdinal(val, worstVal) > 0;

                        if (shouldSwap)
                        {
                            queue.Dequeue();
                            queue.Enqueue(i, val);
                        }
                    }
                }
            }

            int count = queue.Count;
            for (int i = count - 1; i >= 0; i--) resultBuffer[i] = queue.Dequeue();
            return count;
        }

        private static int[] SortIndicesByColumn(int[] indices, IColumn col, bool ascending)
        {
            if (col is IntColumn ic)
            {
                Array.Sort(indices, (a, b) => (ascending ? 1 : -1) * ic.Get(a).CompareTo(ic.Get(b)));
            }
            else if (col is DoubleColumn dc)
            {
                Array.Sort(indices, (a, b) => (ascending ? 1 : -1) * dc.Get(a).CompareTo(dc.Get(b)));
            }
            else if (col is StringColumn sc)
            {
                Array.Sort(indices, (a, b) => (ascending ? 1 : -1) * string.CompareOrdinal(sc.Get(a), sc.Get(b)));
            }

            return indices;
        }
    }
}