using System.Reflection;
using System.Text;
using LeichtFrame.Core.Expressions;
using LeichtFrame.Core.Operations.Delegates;

namespace LeichtFrame.Core
{
    /// <summary>
    /// Represents a high-performance, column-oriented in-memory data table.
    /// Optimized for low memory allocation and fast analytical queries using SIMD and <see cref="Span{T}"/>.
    /// </summary>
    public class DataFrame : IDisposable
    {
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
                if (_columns.Any(c => c.Length != expectedLength))
                {
                    throw new ArgumentException(
                       $"Column length mismatch. Expected {expectedLength} rows based on first column.");
                }
            }

            // 2. Build Schema automatically from column metadata
            var definitions = _columns.Select(c => new ColumnDefinition(c.Name, c.DataType, c.IsNullable));
            Schema = new DataFrameSchema(definitions);
        }

        /// <summary>
        /// Creates a new, empty DataFrame based on the provided schema.
        /// Pre-allocates memory for the specified capacity to minimize resize operations.
        /// </summary>
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
        /// Creates a DataFrame from a collection of objects (POCOs) using Reflection.
        /// </summary>
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

        // --- INDEXERS & ACCESSORS ---

        /// <summary>
        /// Gets the column at the specified index.
        /// </summary>
        public IColumn this[int index] => _columns[index];

        /// <summary>
        /// Gets the column with the specified name.
        /// </summary>
        public IColumn this[string name] => _columns[Schema.GetColumnIndex(name)];

        /// <summary>
        /// Tries to get the column with the specified name.
        /// </summary>
        public bool TryGetColumn(string name, out IColumn? column)
        {
            if (Schema.HasColumn(name))
            {
                column = this[name];
                return true;
            }
            column = null;
            return false;
        }

        /// <summary>
        /// Returns the names of all columns in the DataFrame.
        /// </summary>
        public IEnumerable<string> GetColumnNames() => _columns.Select(c => c.Name);

        /// <summary>
        /// Returns the .NET Type of the data stored in the specified column.
        /// </summary>
        public Type GetColumnType(string name) => this[name].DataType;

        /// <summary>
        /// Checks if a column with the given name exists in the DataFrame.
        /// </summary>
        public bool HasColumn(string name) => Schema.HasColumn(name);

        // =========================================================
        // EAGER OPERATIONS (Direct Execution)
        // =========================================================

        /// <summary>
        /// Filters rows using a C# delegate. Executed immediately via row-by-row iteration.
        /// Use only if logic cannot be expressed via <see cref="Lazy"/>.
        /// </summary>
        /// <param name="predicate">The filter condition.</param>
        /// <returns>A new DataFrame containing matching rows.</returns>
        public DataFrame Where(Func<RowView, bool> predicate)
        {
            // Delegates to the specialized Eager implementation in Operations/Delegates
            return FilterDelegateOps.Execute(this, predicate);
        }

        /// <summary>
        /// Selects columns immediately. Uses the high-performance Lazy Engine internally.
        /// </summary>
        /// <param name="columnNames">The names of the columns to keep.</param>
        public DataFrame Select(params string[] columnNames)
        {
            return this.Lazy().Select(columnNames).Collect();
        }

        /// <summary>
        /// Sorts the DataFrame immediately in ascending order.
        /// </summary>
        public DataFrame OrderBy(string columnName)
        {
            return this.Lazy().OrderBy(columnName).Collect();
        }

        /// <summary>
        /// Sorts the DataFrame immediately in descending order.
        /// </summary>
        public DataFrame OrderByDescending(string columnName)
        {
            return this.Lazy().OrderByDescending(columnName).Collect();
        }

        // =========================================================
        // LAZY ENTRY POINT
        // =========================================================

        /// <summary>
        /// Converts this DataFrame into a <see cref="LazyDataFrame"/> to enable 
        /// optimization and lazy evaluation of subsequent operations.
        /// </summary>
        /// <returns>A lazy wrapper around this DataFrame.</returns>
        public LazyDataFrame Lazy()
        {
            return LazyDataFrame.From(this);
        }

        // --- UTILS & DISPOSE ---

        /// <summary>
        /// Returns a short summary of the DataFrame.
        /// </summary>
        public override string ToString()
        {
            return $"DataFrame ({RowCount} rows, {ColumnCount} columns)";
        }

        /// <summary>
        /// Generates a formatted string representing the first N rows of the DataFrame.
        /// </summary>
        public string Inspect(int limit = 10)
        {
            if (ColumnCount == 0) return "Empty DataFrame";

            var sb = new StringBuilder();
            sb.AppendLine(ToString());
            sb.AppendLine(new string('-', 30));

            int rowsToShow = Math.Min(RowCount, limit);
            int[] widths = new int[ColumnCount];

            // Calculate widths
            for (int c = 0; c < ColumnCount; c++)
            {
                var col = _columns[c];
                int maxWidth = Math.Max(col.Name.Length, col.DataType.Name.Length + 2);
                for (int r = 0; r < rowsToShow; r++)
                {
                    int len = col.GetValue(r)?.ToString()?.Length ?? 4;
                    if (len > maxWidth) maxWidth = len;
                }
                widths[c] = Math.Min(maxWidth, 50) + 2;
            }

            // Header
            for (int c = 0; c < ColumnCount; c++) sb.Append(_columns[c].Name.PadRight(widths[c]));
            sb.AppendLine();
            for (int c = 0; c < ColumnCount; c++) sb.Append($"<{_columns[c].DataType.Name}>".PadRight(widths[c]));
            sb.AppendLine();
            sb.AppendLine(new string('-', widths.Sum()));

            // Rows
            for (int r = 0; r < rowsToShow; r++)
            {
                for (int c = 0; c < ColumnCount; c++)
                {
                    object? val = _columns[c].GetValue(r);
                    string valStr = val is null ? "null" : val.ToString() ?? "";
                    if (valStr.Length > widths[c] - 1) valStr = valStr.Substring(0, widths[c] - 4) + "...";
                    sb.Append(valStr.PadRight(widths[c]));
                }
                sb.AppendLine();
            }

            if (RowCount > limit)
            {
                sb.AppendLine(new string('-', 20));
                sb.AppendLine($"... ({RowCount - limit} more rows)");
            }

            return sb.ToString();
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
        protected virtual void Dispose(bool disposing)
        {
            if (_isDisposed) return;

            if (disposing)
            {
                foreach (var col in _columns)
                {
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