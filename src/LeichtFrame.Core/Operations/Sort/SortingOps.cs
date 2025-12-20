namespace LeichtFrame.Core.Operations.Sort
{
    /// <summary>
    /// Provides sorting functionality for DataFrames and Columns.
    /// </summary>
    public static class SortingOps
    {
        /// <summary>
        /// Returns an array of row indices sorted by the values in the specified column.
        /// Does not modify the original data.
        /// </summary>
        /// <param name="column">The column to sort by.</param>
        /// <param name="ascending">If true, sorts from smallest to largest. If false, largest to smallest.</param>
        /// <returns>An array of integers representing the new row order.</returns>
        public static int[] GetSortedIndices(this IColumn column, bool ascending = true)
        {
            // 1. Initialize indices [0, 1, 2, ... N-1]
            int[] indices = new int[column.Length];
            for (int i = 0; i < indices.Length; i++)
            {
                indices[i] = i;
            }

            // 2. Select optimized comparer based on column type
            IComparer<int> comparer;

            if (column is IntColumn ic)
            {
                comparer = new IntIndirectComparer(ic, ascending);
            }
            else if (column is DoubleColumn dc)
            {
                comparer = new DoubleIndirectComparer(dc, ascending);
            }
            else if (column is StringColumn sc)
            {
                comparer = new StringIndirectComparer(sc, ascending);
            }
            else if (column is DateTimeColumn dtc)
            {
                comparer = new DateTimeIndirectComparer(dtc, ascending);
            }
            else if (column is BoolColumn bc)
            {
                comparer = new BoolIndirectComparer(bc, ascending);
            }
            else
            {
                // Fallback for unknown types (slow via object boxing)
                comparer = new ObjectIndirectComparer(column, ascending);
            }

            // 3. Sort indices indirectly
            // Note: Array.Sort is not stable, but it's fast (Introsort)
            Array.Sort(indices, comparer);

            return indices;
        }

        // --- Comparers ---

        // Helper to handle null logic: Nulls usually come first (smallest)
        private static int CompareNulls(bool isNullX, bool isNullY)
        {
            if (isNullX && isNullY) return 0;
            if (isNullX) return -1; // Null is smaller than Value
            return 1;               // Value is larger than Null
        }

        private readonly struct IntIndirectComparer : IComparer<int>
        {
            private readonly IntColumn _col;
            private readonly int _direction; // 1 for asc, -1 for desc

            public IntIndirectComparer(IntColumn col, bool asc)
            {
                _col = col;
                _direction = asc ? 1 : -1;
            }

            public int Compare(int x, int y)
            {
                bool nx = _col.IsNull(x);
                bool ny = _col.IsNull(y);

                if (nx || ny) return CompareNulls(nx, ny) * _direction;

                int valX = _col.Get(x);
                int valY = _col.Get(y);

                return valX.CompareTo(valY) * _direction;
            }
        }

        private readonly struct DoubleIndirectComparer : IComparer<int>
        {
            private readonly DoubleColumn _col;
            private readonly int _direction;

            public DoubleIndirectComparer(DoubleColumn col, bool asc)
            {
                _col = col;
                _direction = asc ? 1 : -1;
            }

            public int Compare(int x, int y)
            {
                bool nx = _col.IsNull(x);
                bool ny = _col.IsNull(y);

                if (nx || ny) return CompareNulls(nx, ny) * _direction;

                double valX = _col.Get(x);
                double valY = _col.Get(y);

                return valX.CompareTo(valY) * _direction;
            }
        }

        private readonly struct StringIndirectComparer : IComparer<int>
        {
            private readonly StringColumn _col;
            private readonly int _direction;

            public StringIndirectComparer(StringColumn col, bool asc)
            {
                _col = col;
                _direction = asc ? 1 : -1;
            }

            public int Compare(int x, int y)
            {
                return _col.CompareRaw(x, y) * _direction;
            }
        }

        private readonly struct DateTimeIndirectComparer : IComparer<int>
        {
            private readonly DateTimeColumn _col;
            private readonly int _direction;

            public DateTimeIndirectComparer(DateTimeColumn col, bool asc)
            {
                _col = col;
                _direction = asc ? 1 : -1;
            }

            public int Compare(int x, int y)
            {
                bool nx = _col.IsNull(x);
                bool ny = _col.IsNull(y);

                if (nx || ny) return CompareNulls(nx, ny) * _direction;

                return _col.Get(x).CompareTo(_col.Get(y)) * _direction;
            }
        }

        private readonly struct BoolIndirectComparer : IComparer<int>
        {
            private readonly BoolColumn _col;
            private readonly int _direction;

            public BoolIndirectComparer(BoolColumn col, bool asc)
            {
                _col = col;
                _direction = asc ? 1 : -1;
            }

            public int Compare(int x, int y)
            {
                bool nx = _col.IsNull(x);
                bool ny = _col.IsNull(y);

                if (nx || ny) return CompareNulls(nx, ny) * _direction;

                return _col.Get(x).CompareTo(_col.Get(y)) * _direction;
            }
        }

        private readonly struct ObjectIndirectComparer : IComparer<int>
        {
            private readonly IColumn _col;
            private readonly int _direction;

            public ObjectIndirectComparer(IColumn col, bool asc)
            {
                _col = col;
                _direction = asc ? 1 : -1;
            }

            public int Compare(int x, int y)
            {
                bool nx = _col.IsNull(x);
                bool ny = _col.IsNull(y);

                if (nx || ny) return CompareNulls(nx, ny) * _direction;

                var valX = _col.GetValue(x) as IComparable;
                var valY = _col.GetValue(y) as IComparable;

                if (valX == null && valY == null) return 0;
                if (valX == null) return -1 * _direction;
                if (valY == null) return 1 * _direction;

                return valX.CompareTo(valY) * _direction;
            }
        }
    }
}