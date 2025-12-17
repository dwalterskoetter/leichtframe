namespace LeichtFrame.Core
{
    /// <summary>
    /// Represents a lightweight, read-only view of a single row in a <see cref="DataFrame"/>.
    /// Acts as a zero-copy cursor enabling row-based operations without materializing objects.
    /// </summary>
    public readonly struct RowView
    {
        private readonly int _rowIndex;
        private readonly IReadOnlyList<IColumn> _columns;
        private readonly DataFrameSchema _schema;

        /// <summary>
        /// Initializes a new instance of the <see cref="RowView"/> struct.
        /// </summary>
        /// <param name="rowIndex">The zero-based index of the row.</param>
        /// <param name="columns">The list of columns backing the data.</param>
        /// <param name="schema">The schema definition for column name lookups.</param>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if rowIndex is negative.</exception>
        /// <exception cref="ArgumentNullException">Thrown if columns or schema are null.</exception>
        public RowView(int rowIndex, IReadOnlyList<IColumn> columns, DataFrameSchema schema)
        {
            if (rowIndex < 0) throw new ArgumentOutOfRangeException(nameof(rowIndex));
            _rowIndex = rowIndex;
            _columns = columns ?? throw new ArgumentNullException(nameof(columns));
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        }

        /// <summary>
        /// Gets the strongly-typed value from the specified column index.
        /// This is the fastest way to access data within a row.
        /// </summary>
        /// <typeparam name="T">The expected type of the value.</typeparam>
        /// <param name="columnIndex">The zero-based index of the column.</param>
        /// <returns>The value of type T.</returns>
        /// <exception cref="InvalidCastException">Thrown if the column type does not match T.</exception>
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

        /// <summary>
        /// Gets the strongly-typed value from the column with the specified name.
        /// </summary>
        /// <typeparam name="T">The expected type of the value.</typeparam>
        /// <param name="columnName">The name of the column.</param>
        /// <returns>The value of type T.</returns>
        /// <exception cref="ArgumentException">Thrown if the column name does not exist.</exception>
        public T Get<T>(string columnName)
        {
            int index = _schema.GetColumnIndex(columnName);
            return Get<T>(index);
        }

        /// <summary>
        /// Gets the value at the specified column index as an object (boxed).
        /// </summary>
        /// <param name="columnIndex">The zero-based index of the column.</param>
        /// <returns>The value as an object, or null.</returns>
        public object? GetValue(int columnIndex)
        {
            return _columns[columnIndex].GetValue(_rowIndex);
        }

        // Indexer for convenience

        /// <summary>
        /// Gets the value at the specified column index (untyped).
        /// </summary>
        /// <param name="index">The zero-based column index.</param>
        public object? this[int index] => GetValue(index);

        /// <summary>
        /// Gets the value of the column with the specified name (untyped).
        /// </summary>
        /// <param name="name">The name of the column.</param>
        public object? this[string name] => GetValue(_schema.GetColumnIndex(name));

        /// <summary>
        /// Checks if the value at the specified column index is null.
        /// </summary>
        public bool IsNull(int columnIndex)
        {
            return _columns[columnIndex].IsNull(_rowIndex);
        }

        /// <summary>
        /// Checks if the value at the specified column name is null.
        /// </summary>
        public bool IsNull(string columnName)
        {
            int index = _schema.GetColumnIndex(columnName);
            return IsNull(index);
        }
    }
}