using System.Reflection;
using System.Text;

namespace LeichtFrame.Core
{
    /// <summary>
    /// Represents a high-performance, column-oriented in-memory data table.
    /// Optimized for low memory allocation and fast analytical queries using SIMD and <see cref="Span{T}"/>.
    /// </summary>
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

        /// <summary>
        /// Returns a short summary of the DataFrame (e.g., "DataFrame (1000 rows, 5 columns)").
        /// </summary>
        public override string ToString()
        {
            return $"DataFrame ({RowCount} rows, {ColumnCount} columns)";
        }

        /// <summary>
        /// Generates a formatted string representing the first N rows of the DataFrame.
        /// Useful for console output and debugging.
        /// </summary>
        /// <param name="limit">The maximum number of rows to display (default 10).</param>
        public string Inspect(int limit = 10)
        {
            if (ColumnCount == 0) return "Empty DataFrame";

            var sb = new StringBuilder();
            sb.AppendLine(ToString());
            sb.AppendLine(new string('-', 30));

            int rowsToShow = Math.Min(RowCount, limit);

            // 1. Calculate optimal column widths based on Header and Visible Data
            int[] widths = new int[ColumnCount];
            for (int c = 0; c < ColumnCount; c++)
            {
                var col = _columns[c];
                int maxWidth = col.Name.Length;

                // Consider type name length (e.g., <Int32>)
                maxWidth = Math.Max(maxWidth, col.DataType.Name.Length + 2);

                // Scan visible data for width
                for (int r = 0; r < rowsToShow; r++)
                {
                    object? val = col.GetValue(r);
                    int len = val?.ToString()?.Length ?? 4; // 4 for "null"
                    if (len > maxWidth) maxWidth = len;
                }

                // Limit to a reasonable max (e.g., 50 characters) to prevent console overflow
                widths[c] = Math.Min(maxWidth, 50) + 2; // +2 Padding
            }

            // 2. Print Header (Names)
            for (int c = 0; c < ColumnCount; c++)
            {
                sb.Append(_columns[c].Name.PadRight(widths[c]));
            }
            sb.AppendLine();

            // 3. Print Header (Types)
            for (int c = 0; c < ColumnCount; c++)
            {
                string typeStr = $"<{_columns[c].DataType.Name}>";
                sb.Append(typeStr.PadRight(widths[c]));
            }
            sb.AppendLine();

            // Separator line based on total width
            int totalWidth = widths.Sum();
            sb.AppendLine(new string('-', totalWidth));

            // 4. Print Rows
            for (int r = 0; r < rowsToShow; r++)
            {
                for (int c = 0; c < ColumnCount; c++)
                {
                    object? val = _columns[c].GetValue(r);
                    string valStr = val is null ? "null" : val.ToString() ?? "";

                    // Truncate if too long (Visual Safety)
                    if (valStr.Length > widths[c] - 1)
                        valStr = valStr.Substring(0, widths[c] - 4) + "...";

                    sb.Append(valStr.PadRight(widths[c]));
                }
                sb.AppendLine();
            }

            // 5. Footer hint
            if (RowCount > limit)
            {
                sb.AppendLine(new string('-', 20));
                sb.AppendLine($"... ({RowCount - limit} more rows)");
            }

            return sb.ToString();
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

        /// <summary>
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
        /// Creates a DataFrame from a collection of objects (POCOs) using Reflection.
        /// The schema is automatically generated from the public properties of <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of the objects, determining the schema.</typeparam>
        /// <param name="objects">The collection of objects to load.</param>
        /// <returns>A populated DataFrame containing the data from the objects.</returns>
        public static DataFrame FromObjects<T>(IEnumerable<T> objects)
        {
            if (objects == null) throw new ArgumentNullException(nameof(objects));

            // 1. Get Schema via centralized logic
            var schema = DataFrameSchema.FromType<T>();

            // 2. Prepare for data population
            int estimatedCount = objects is ICollection<T> coll ? coll.Count : 16;
            var df = DataFrame.Create(schema, estimatedCount);

            // Cache PropertyInfos for speed
            var type = typeof(T);
            var propMap = new Dictionary<string, PropertyInfo>();
            foreach (var col in df.Columns)
            {
                propMap[col.Name] = type.GetProperty(col.Name)!;
            }

            // 3. Populate Data
            foreach (var item in objects)
            {
                foreach (var col in df.Columns)
                {
                    var prop = propMap[col.Name];
                    object? val = prop.GetValue(item);
                    col.AppendObject(val);
                }
            }

            return df;
        }

        /// <summary>
        /// Disposes all contained columns, returning their memory to the pool.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
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

        /// <summary>
        /// Checks if a column with the given name exists in the DataFrame.
        /// </summary>
        public bool HasColumn(string name)
        {
            if (string.IsNullOrEmpty(name)) return false;
            return Schema.HasColumn(name);
        }

        /// <summary>
        /// Returns the names of all columns in the DataFrame.
        /// </summary>
        public IEnumerable<string> GetColumnNames()
        {
            return _columns.Select(c => c.Name);
        }

        /// <summary>
        /// Returns the .NET Type of the data stored in the specified column.
        /// Throws ArgumentException if the column does not exist.
        /// </summary>
        public Type GetColumnType(string name)
        {
            // We use the existing indexer, which already handles validation/exception
            return this[name].DataType;
        }

        /// <summary>
        /// Converts this DataFrame into a <see cref="LazyDataFrame"/> to enable 
        /// optimization and lazy evaluation of subsequent operations.
        /// </summary>
        /// <returns>A lazy wrapper around this DataFrame.</returns>
        public LazyDataFrame Lazy()
        {
            return LazyDataFrame.From(this);
        }
    }
}