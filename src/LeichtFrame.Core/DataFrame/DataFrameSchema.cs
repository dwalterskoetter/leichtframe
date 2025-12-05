using System.Text.Json;

namespace LeichtFrame.Core;

/// Defines the metadata for a single column.
public record ColumnDefinition(string Name, Type DataType, bool IsNullable = false);

/// Represents the structure of a DataFrame (collection of column definitions).
public class DataFrameSchema
{
    private readonly List<ColumnDefinition> _columns;
    private readonly Dictionary<string, int> _nameMap;

    public IReadOnlyList<ColumnDefinition> Columns => _columns;

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

    /// Checks if a column with the given name exists.
    public bool HasColumn(string name) => _nameMap.ContainsKey(name);

    /// Gets the index of the column. Throws if not found.
    public int GetColumnIndex(string name)
    {
        if (_nameMap.TryGetValue(name, out int index))
            return index;

        throw new ArgumentException($"Column '{name}' does not exist in the schema.");
    }

    // --- JSON Serialization Logic ---
    /// Serializes the schema to a JSON string.
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

    /// Creates a Schema from a JSON string.
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
    /// Helper to get the type of a column by name.
    /// </summary>
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
    /// Creates a schema definition automatically from a C# class (POCO).
    /// </summary>
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