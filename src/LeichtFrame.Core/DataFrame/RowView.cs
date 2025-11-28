namespace LeichtFrame.Core
{
    public readonly struct RowView
    {
        private readonly int _rowIndex;
        private readonly IReadOnlyList<IColumn> _columns;
        private readonly DataFrameSchema _schema;

        public RowView(int rowIndex, IReadOnlyList<IColumn> columns, DataFrameSchema schema)
        {
            if (rowIndex < 0) throw new ArgumentOutOfRangeException(nameof(rowIndex));
            _rowIndex = rowIndex;
            _columns = columns ?? throw new ArgumentNullException(nameof(columns));
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        }

        /// Typed Access (Fast, direct Interface call).
        public T Get<T>(int columnIndex)
        {
            var col = _columns[columnIndex];

            // Pattern matching on generic interface
            if (col is IColumn<T> typedCol)
            {
                return typedCol.GetValue(_rowIndex);
            }

            throw new InvalidCastException(
                $"Column '{col.Name}' is type {col.DataType.Name}, but '{typeof(T).Name}' was requested.");
        }

        public T Get<T>(string columnName)
        {
            int index = _schema.GetColumnIndex(columnName);
            return Get<T>(index);
        }

        /// Untyped Access (Boxing).
        public object? GetValue(int columnIndex)
        {
            return _columns[columnIndex].GetValue(_rowIndex);
        }

        // Indexer for convenience
        public object? this[int index] => GetValue(index);
        public object? this[string name] => GetValue(_schema.GetColumnIndex(name));
    }
}