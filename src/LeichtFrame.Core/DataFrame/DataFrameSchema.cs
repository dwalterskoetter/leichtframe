using System.Text.Json;

namespace LeichtFrame.Core;

/// <summary>
/// Defines the metadata for a single column within a DataFrame schema.
/// </summary>
/// <param name="Name">The unique name of the column.</param>
/// <param name="DataType">The CLR type of the data stored in the column.</param>
/// <param name="IsNullable">Indicates if the column supports null values.</param>
/// <param name="SourceIndex">The index of the column in the original data source (if applicable).</param>
public record ColumnDefinition(string Name, Type DataType, bool IsNullable = false, int? SourceIndex = null);

/// <summary>
/// Represents the structure of a DataFrame, consisting of a collection of column definitions.
/// Provides lookup methods for column indices and types.
/// </summary>
public class DataFrameSchema
{
    private readonly List<ColumnDefinition> _columns;
    private readonly Dictionary<string, int> _nameMap;

    /// <summary>
    /// Gets the list of column definitions in this schema.
    /// </summary>
    public IReadOnlyList<ColumnDefinition> Columns => _columns;

    /// <summary>
    /// Initializes a new instance of the <see cref="DataFrameSchema"/> class.
    /// </summary>
    /// <param name="columns">The collection of column definitions.</param>
    /// <exception cref="ArgumentNullException">Thrown if columns is null.</exception>
    /// <exception cref="ArgumentException">Thrown if duplicate column names are detected.</exception>
    public DataFrameSchema(IEnumerable<ColumnDefinition> columns)
    {
        _columns = columns?.ToList() ?? throw new ArgumentNullException(nameof(columns));
        _nameMap = new Dictionary<string, int>();

        // Build lookup dictionary for fast access
        for (int i = 0; i < _columns.Count; i++)
        {
            var col = _columns[i];
            if (_nameMap.ContainsKey(col.Name))
                throw new ArgumentException($"Duplicate column name '{col.Name}' is not allowed.");

            _nameMap[col.Name] = i;
        }
    }

    /// <summary>
    /// Checks if a column with the given name exists in the schema.
    /// </summary>
    /// <param name="name">The name of the column to check.</param>
    /// <returns><c>true</c> if the column exists; otherwise, <c>false</c>.</returns>
    public bool HasColumn(string name) => _nameMap.ContainsKey(name);

    /// <summary>
    /// Gets the zero-based index of the column with the specified name.
    /// </summary>
    /// <param name="name">The name of the column.</param>
    /// <returns>The index of the column.</returns>
    /// <exception cref="ArgumentException">Thrown if the column does not exist.</exception>
    public int GetColumnIndex(string name)
    {
        if (_nameMap.TryGetValue(name, out int index))
            return index;

        throw new ArgumentException($"Column '{name}' does not exist in the schema.");
    }

    // --- JSON Serialization Logic ---

    /// <summary>
    /// Serializes the schema to a JSON string representation.
    /// Useful for persisting metadata or transferring schemas between processes.
    /// </summary>
    /// <returns>A JSON string defining the schema.</returns>
    public string ToJson()
    {
        // Convert to DTO because System.Type implies security risks and complexity in raw JSON. We store the Type Name as a string.
        var dto = new SchemaDto
        {
            Columns = _columns.Select(c => new ColumnDto
            {
                Name = c.Name,
                DataTypeName = c.DataType.AssemblyQualifiedName ?? c.DataType.FullName ?? c.DataType.Name,
                IsNullable = c.IsNullable
            }).ToList()
        };

        return JsonSerializer.Serialize(dto, new JsonSerializerOptions { WriteIndented = true });
    }

    /// <summary>
    /// Creates a <see cref="DataFrameSchema"/> from a JSON string.
    /// </summary>
    /// <param name="json">The JSON string containing the schema definition.</param>
    /// <returns>The deserialized schema.</returns>
    /// <exception cref="ArgumentException">Thrown if the JSON is invalid.</exception>
    public static DataFrameSchema FromJson(string json)
    {
        var dto = JsonSerializer.Deserialize<SchemaDto>(json);
        if (dto == null || dto.Columns == null)
            throw new ArgumentException("Invalid JSON schema.");

        var definitions = dto.Columns.Select(c => new ColumnDefinition(
            c.Name,
            Type.GetType(c.DataTypeName) ?? throw new InvalidOperationException($"Type '{c.DataTypeName}' not found."),
            c.IsNullable
        ));

        return new DataFrameSchema(definitions);
    }

    /// <summary>
    /// Helper to get the <see cref="Type"/> of a column by name.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <returns>The data type of the column.</returns>
    /// <exception cref="ArgumentException">Thrown if the column does not exist.</exception>
    public Type GetColumnType(string name)
    {
        int index = GetColumnIndex(name); // Wirft Fehler, wenn nicht gefunden
        return _columns[index].DataType;
    }

    // --- Private Helper Classes for JSON ---
    private class SchemaDto
    {
        public List<ColumnDto> Columns { get; set; } = new();
    }

    private class ColumnDto
    {
        public string Name { get; set; } = string.Empty;
        public string DataTypeName { get; set; } = string.Empty;
        public bool IsNullable { get; set; }
    }

    /// <summary>
    /// Creates a schema definition automatically from a C# class (POCO) using Reflection.
    /// Only supported primitive types (int, double, bool, string, DateTime) are mapped.
    /// </summary>
    /// <typeparam name="T">The POCO type to analyze.</typeparam>
    /// <returns>A derived <see cref="DataFrameSchema"/>.</returns>
    /// <exception cref="ArgumentException">Thrown if the type has no supported public properties.</exception>
    public static DataFrameSchema FromType<T>()
    {
        var properties = typeof(T).GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
        var colDefs = new List<ColumnDefinition>();

        foreach (var prop in properties)
        {
            Type type = prop.PropertyType;
            // Unbox Nullable<T> -> T
            Type coreType = Nullable.GetUnderlyingType(type) ?? type;
            bool isNullable = !type.IsValueType || Nullable.GetUnderlyingType(type) != null;

            // Supported Types Check
            if (coreType != typeof(int) && coreType != typeof(double) &&
                coreType != typeof(string) && coreType != typeof(bool) &&
                coreType != typeof(DateTime))
            {
                continue; // Skip unsupported types
            }

            colDefs.Add(new ColumnDefinition(prop.Name, coreType, isNullable));
        }

        if (colDefs.Count == 0)
            throw new ArgumentException($"Type '{typeof(T).Name}' has no supported public properties.");

        return new DataFrameSchema(colDefs);
    }
}