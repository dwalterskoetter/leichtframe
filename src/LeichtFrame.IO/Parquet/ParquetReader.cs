using LeichtFrame.Core;
using Parquet.Schema;

namespace LeichtFrame.IO
{
    /// <summary>
    /// Provides high-performance methods to read Apache Parquet files into a <see cref="DataFrame"/>.
    /// Automatically maps Parquet schema types to LeichtFrame column types.
    /// </summary>
    public static class ParquetReader
    {
        /// <summary>
        /// Reads a Parquet file from the specified file path.
        /// </summary>
        /// <param name="path">The full path to the Parquet file.</param>
        /// <returns>A populated <see cref="DataFrame"/> containing the data.</returns>
        public static DataFrame Read(string path)
        {
            using var stream = File.OpenRead(path);
            return Read(stream);
        }

        /// <summary>
        /// Reads a Parquet file from a stream synchronously.
        /// </summary>
        /// <param name="stream">The input stream containing Parquet data.</param>
        /// <returns>A populated <see cref="DataFrame"/>.</returns>
        public static DataFrame Read(Stream stream)
        {
            // Synchronous Wrapper for the Async method
            return ReadAsync(stream).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Reads a Parquet file from a stream asynchronously.
        /// Recommended for I/O-bound operations in Web APIs to avoid blocking threads.
        /// </summary>
        /// <param name="stream">The input stream containing Parquet data.</param>
        /// <returns>A task that represents the asynchronous read operation, containing the resulting <see cref="DataFrame"/>.</returns>
        public static async Task<DataFrame> ReadAsync(Stream stream)
        {
            using var reader = await Parquet.ParquetReader.CreateAsync(stream);

            // 1. Schema Mapping (Parquet -> LeichtFrame)
            var dataFields = reader.Schema.GetDataFields();
            var colDefs = dataFields.Select(f => MapToColumnDefinition(f));
            var schema = new DataFrameSchema(colDefs);

            // 2. Create DataFrame (RowCount is known in metadata header!)
            // Parquet stores rows per RowGroup. We sum or take capacity.
            // For simplicity, we start empty and let Append work.
            var df = DataFrame.Create(schema, capacity: 1000); // Tuning option for later

            // 3. Read Data (RowGroup by RowGroup)
            for (int i = 0; i < reader.RowGroupCount; i++)
            {
                using var groupReader = reader.OpenRowGroupReader(i);

                foreach (var field in dataFields)
                {
                    var column = df[field.Name];

                    // Reads the entire column of this RowGroup as an array
                    var parquetColumn = await groupReader.ReadColumnAsync(field);

                    // 4. Copy Data to LeichtFrame Column
                    AppendData(column, parquetColumn.Data);
                }
            }

            return df;
        }

        private static ColumnDefinition MapToColumnDefinition(DataField field)
        {
            // Mapping Parquet Types -> .NET Types
            Type targetType = field.ClrNullableIfHasNullsType;

            // We want the core type for LeichtFrame (int instead of int?) + IsNullable flag
            Type coreType = Nullable.GetUnderlyingType(targetType) ?? targetType;
            bool isNullable = field.IsNullable || targetType.IsGenericType; // rough rule

            // Support Check
            if (coreType != typeof(int) && coreType != typeof(double) &&
                coreType != typeof(string) && coreType != typeof(bool) &&
                coreType != typeof(DateTime))
            {
                // Fallback or Error? Parquet has many types (Decimal, Float...).
                // MVP: We throw an error for unsupported types.
                throw new NotSupportedException($"Parquet type '{coreType.Name}' for column '{field.Name}' is not supported yet.");
            }

            return new ColumnDefinition(field.Name, coreType, isNullable);
        }

        private static void AppendData(IColumn col, Array data)
        {
            // The array from Parquet.Net is typed (e.g., int[] or int?[])
            // We iterate and append.
            // Performance note: In phase 2, we could use low-level Array.Copy here,
            // if the types match exactly (zero-copy or bulk-copy).

            if (col is IntColumn ic)
            {
                foreach (var item in data) ic.Append((int?)item);
            }
            else if (col is DoubleColumn dc)
            {
                foreach (var item in data) dc.Append((double?)item);
            }
            else if (col is StringColumn sc)
            {
                foreach (var item in data) sc.Append((string?)item);
            }
            else if (col is BoolColumn bc)
            {
                foreach (var item in data) bc.Append((bool?)item);
            }
            else if (col is DateTimeColumn dtc)
            {
                foreach (var item in data) dtc.Append((DateTime?)item);
            }
        }
    }
}