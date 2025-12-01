namespace LeichtFrame.Core
{
    public class DataFrame : IDisposable
    {
        /// <summary>
        /// Creates a new, empty DataFrame based on the provided schema.
        /// Pre-allocates memory for the specified capacity to minimize resize operations.
        /// </summary>
        /// <param name="schema">The schema defining the columns.</param>
        /// <param name="capacity">The initial capacity (number of rows) to reserve.</param>
        /// <returns>A new DataFrame instance.</returns>
        public static DataFrame Create(DataFrameSchema schema, int capacity = 16)
        {
            if (schema == null) throw new ArgumentNullException(nameof(schema));
            if (capacity < 0) throw new ArgumentOutOfRangeException(nameof(capacity));

            var columns = new List<IColumn>(schema.Columns.Count);

            foreach (var colDef in schema.Columns)
            {
                var col = ColumnFactory.Create(colDef.Name, colDef.DataType, capacity, colDef.IsNullable);
                columns.Add(col);
            }

            return new DataFrame(columns);
        }

        private readonly List<IColumn> _columns;
        private bool _isDisposed;

        /// <summary>
        /// Gets the schema definition of this DataFrame.
        /// </summary>
        public DataFrameSchema Schema { get; }

        /// <summary>
        /// Gets the internal list of columns.
        /// </summary>
        public IReadOnlyList<IColumn> Columns => _columns;

        /// <summary>
        /// Gets the number of rows in the DataFrame.
        /// </summary>
        public int RowCount => _columns.Count > 0 ? _columns[0].Length : 0;

        /// <summary>
        /// Gets the number of columns in the DataFrame.
        /// </summary>
        public int ColumnCount => _columns.Count;

        /// <summary>
        /// Creates a new DataFrame from the provided columns.
        /// Validates that all columns share the same length.
        /// </summary>
        public DataFrame(IEnumerable<IColumn> columns)
        {
            if (columns == null) throw new ArgumentNullException(nameof(columns));

            _columns = columns.ToList();

            // 1. Validation: Row Count Consistency
            if (_columns.Count > 0)
            {
                int expectedLength = _columns[0].Length;
                for (int i = 1; i < _columns.Count; i++)
                {
                    if (_columns[i].Length != expectedLength)
                    {
                        throw new ArgumentException(
                            $"Column length mismatch. Column '{_columns[i].Name}' has length {_columns[i].Length}, " +
                            $"but expected {expectedLength} (from '{_columns[0].Name}').");
                    }
                }
            }

            // 2. Build Schema automatically from column metadata
            var definitions = _columns.Select(c => new ColumnDefinition(c.Name, c.DataType, c.IsNullable));
            Schema = new DataFrameSchema(definitions);
        }

        // <summary>
        /// Gets the column at the specified index.
        /// </summary>
        public IColumn this[int index] => _columns[index];

        /// <summary>
        /// Gets the column with the specified name.
        /// Throws <see cref="ArgumentException"/> if the column does not exist.
        /// </summary>
        public IColumn this[string name]
        {
            get
            {
                // Schema lookup handles the exception if name is missing
                int index = Schema.GetColumnIndex(name);
                return _columns[index];
            }
        }

        /// <summary>
        /// Tries to get the column with the specified name.
        /// Returns true if found, otherwise false.
        /// </summary>
        public bool TryGetColumn(string name, out IColumn? column)
        {
            if (Schema.HasColumn(name))
            {
                int index = Schema.GetColumnIndex(name);
                column = _columns[index];
                return true;
            }

            column = null;
            return false;
        }

        /// <summary>
        /// Disposes all contained columns, returning their memory to the pool.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_isDisposed) return;

            if (disposing)
            {
                foreach (var col in _columns)
                {
                    // Check if the column implements IDisposable (our concrete columns do)
                    if (col is IDisposable disposableCol)
                    {
                        disposableCol.Dispose();
                    }
                }
            }
            _isDisposed = true;
        }
    }
}