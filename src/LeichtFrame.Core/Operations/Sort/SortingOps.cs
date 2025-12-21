namespace LeichtFrame.Core.Operations.Sort
{
    /// <summary>
    /// Provides sorting functionality for DataFrames and Columns.
    /// </summary>
    public static class SortingOps
    {
        /// <summary>
        /// Returns sorted indices for a single column.
        /// </summary>
        public static int[] GetSortedIndices(this IColumn column, bool ascending = true)
        {
            int[] indices = new int[column.Length];
            for (int i = 0; i < indices.Length; i++) indices[i] = i;

            IComparer<int> comparer = CreateComparer(column, ascending);
            Array.Sort(indices, comparer);

            return indices;
        }

        // --- NEU: Multi-Column Support ---

        /// <summary>
        /// Returns sorted indices based on multiple columns (stable sort logic).
        /// </summary>
        public static int[] GetSortedIndices(this IColumn[] columns, bool[] ascending)
        {
            if (columns == null || columns.Length == 0) throw new ArgumentException("No columns provided");
            int length = columns[0].Length;
            int[] indices = new int[length];
            for (int i = 0; i < length; i++) indices[i] = i;

            var comparer = new MultiColumnIndirectComparer(columns, ascending);
            Array.Sort(indices, comparer);

            return indices;
        }

        // --- Comparers ---

        private static IComparer<int> CreateComparer(IColumn column, bool asc)
        {
            if (column is IntColumn ic) return new IntIndirectComparer(ic, asc);
            if (column is DoubleColumn dc) return new DoubleIndirectComparer(dc, asc);
            if (column is StringColumn sc) return new StringIndirectComparer(sc, asc);
            if (column is DateTimeColumn dtc) return new DateTimeIndirectComparer(dtc, asc);
            if (column is BoolColumn bc) return new BoolIndirectComparer(bc, asc);
            return new ObjectIndirectComparer(column, asc);
        }

        internal readonly struct MultiColumnIndirectComparer : IComparer<int>
        {
            private readonly IComparer<int>[] _comparers;

            public MultiColumnIndirectComparer(IColumn[] cols, bool[] ascending)
            {
                _comparers = new IComparer<int>[cols.Length];
                for (int i = 0; i < cols.Length; i++)
                {
                    _comparers[i] = CreateComparer(cols[i], ascending[i]);
                }
            }

            public int Compare(int x, int y)
            {
                for (int i = 0; i < _comparers.Length; i++)
                {
                    int cmp = _comparers[i].Compare(x, y);
                    if (cmp != 0) return cmp;
                }
                return 0;
            }
        }

        private static int CompareNulls(bool isNullX, bool isNullY)
        {
            if (isNullX && isNullY) return 0;
            if (isNullX) return -1;
            return 1;
        }

        private readonly struct IntIndirectComparer : IComparer<int>
        {
            private readonly IntColumn _col;
            private readonly int _direction;
            public IntIndirectComparer(IntColumn col, bool asc) { _col = col; _direction = asc ? 1 : -1; }
            public int Compare(int x, int y)
            {
                bool nx = _col.IsNull(x); bool ny = _col.IsNull(y);
                if (nx || ny) return CompareNulls(nx, ny) * _direction;
                return _col.Get(x).CompareTo(_col.Get(y)) * _direction;
            }
        }

        private readonly struct DoubleIndirectComparer : IComparer<int>
        {
            private readonly DoubleColumn _col;
            private readonly int _direction;
            public DoubleIndirectComparer(DoubleColumn col, bool asc) { _col = col; _direction = asc ? 1 : -1; }
            public int Compare(int x, int y)
            {
                bool nx = _col.IsNull(x); bool ny = _col.IsNull(y);
                if (nx || ny) return CompareNulls(nx, ny) * _direction;
                return _col.Get(x).CompareTo(_col.Get(y)) * _direction;
            }
        }

        private readonly struct StringIndirectComparer : IComparer<int>
        {
            private readonly StringColumn _col;
            private readonly int _direction;
            public StringIndirectComparer(StringColumn col, bool asc) { _col = col; _direction = asc ? 1 : -1; }
            public int Compare(int x, int y) { return _col.CompareRaw(x, y) * _direction; }
        }

        private readonly struct DateTimeIndirectComparer : IComparer<int>
        {
            private readonly DateTimeColumn _col;
            private readonly int _direction;
            public DateTimeIndirectComparer(DateTimeColumn col, bool asc) { _col = col; _direction = asc ? 1 : -1; }
            public int Compare(int x, int y)
            {
                bool nx = _col.IsNull(x); bool ny = _col.IsNull(y);
                if (nx || ny) return CompareNulls(nx, ny) * _direction;
                return _col.Get(x).CompareTo(_col.Get(y)) * _direction;
            }
        }

        private readonly struct BoolIndirectComparer : IComparer<int>
        {
            private readonly BoolColumn _col;
            private readonly int _direction;
            public BoolIndirectComparer(BoolColumn col, bool asc) { _col = col; _direction = asc ? 1 : -1; }
            public int Compare(int x, int y)
            {
                bool nx = _col.IsNull(x); bool ny = _col.IsNull(y);
                if (nx || ny) return CompareNulls(nx, ny) * _direction;
                return _col.Get(x).CompareTo(_col.Get(y)) * _direction;
            }
        }

        private readonly struct ObjectIndirectComparer : IComparer<int>
        {
            private readonly IColumn _col;
            private readonly int _direction;
            public ObjectIndirectComparer(IColumn col, bool asc) { _col = col; _direction = asc ? 1 : -1; }
            public int Compare(int x, int y)
            {
                bool nx = _col.IsNull(x); bool ny = _col.IsNull(y);
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