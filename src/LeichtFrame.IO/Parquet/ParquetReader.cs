using LeichtFrame.Core;
using Parquet.Schema;
using Parquet;

namespace LeichtFrame.IO
{
    /// <summary>
    /// Provides high-performance methods to read Apache Parquet files into a <see cref="DataFrame"/>.
    /// Automatically maps Parquet schema types to LeichtFrame column types.
    /// Supports both full-load and batched streaming (RowGroup-based).
    /// </summary>
    public static class ParquetReader
    {
        // =======================================================================
        // STANDARD READ METHODS (Full Load)
        // =======================================================================

        /// <summary>
        /// Reads a Parquet file from the specified file path.
        /// </summary>
        /// <param name="path">The full path to the Parquet file.</param>
        /// <returns>A populated <see cref="DataFrame"/> containing all data.</returns>
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

            // 2. Create DataFrame
            // We start with a capacity estimate. Parquet has metadata for total rows, but reader.ThriftMetadata might be internal.
            // Safe default.
            var df = DataFrame.Create(schema, capacity: 1000);

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

        // =======================================================================
        // BATCHED READ METHODS (Streaming)
        // =======================================================================

        /// <summary>
        /// Reads a Parquet file in batches, mapping 1 Parquet RowGroup to 1 DataFrame.
        /// This allows processing files larger than available memory.
        /// </summary>
        /// <param name="path">The file path.</param>
        /// <returns>An enumerable of DataFrames.</returns>
        public static IEnumerable<DataFrame> ReadBatches(string path)
        {
            using var stream = File.OpenRead(path);
            foreach (var batch in ReadBatches(stream))
            {
                yield return batch;
            }
        }

        /// <summary>
        /// Reads Parquet batches from a stream synchronously.
        /// Note: This performs blocking calls on the underlying async Parquet library.
        /// </summary>
        /// <param name="stream">The input stream.</param>
        /// <returns>An enumerable of DataFrames.</returns>
        public static IEnumerable<DataFrame> ReadBatches(Stream stream)
        {
            // 1. Open Reader (Blocking wait)
            var task = Parquet.ParquetReader.CreateAsync(stream);
            task.Wait();
            using var reader = task.Result;

            // 2. Schema Mapping
            var dataFields = reader.Schema.GetDataFields();
            var colDefs = dataFields.Select(f => MapToColumnDefinition(f));
            var schema = new DataFrameSchema(colDefs);

            // 3. Iterate RowGroups
            for (int i = 0; i < reader.RowGroupCount; i++)
            {
                using var groupReader = reader.OpenRowGroupReader(i);

                // If RowGroup is empty, we skip or produce empty DF. Parquet usually doesn't store empty groups.
                long groupRowCount = groupReader.RowCount;

                // Create a fresh DataFrame for this batch
                var batchDf = DataFrame.Create(schema, (int)groupRowCount);

                // Read all columns for this group
                foreach (var field in dataFields)
                {
                    var column = batchDf[field.Name];

                    // Read Column Data (Blocking wait)
                    var readTask = groupReader.ReadColumnAsync(field);
                    readTask.Wait();
                    var parquetColumn = readTask.Result;

                    AppendData(column, parquetColumn.Data);
                }

                yield return batchDf;
            }
        }

        // =======================================================================
        // INTERNAL HELPERS
        // =======================================================================

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
                // Fallback for types not strictly typed in our system
                throw new NotSupportedException($"Parquet type '{coreType.Name}' for column '{field.Name}' is not supported yet.");
            }

            return new ColumnDefinition(field.Name, coreType, isNullable);
        }

        private static void AppendData(IColumn col, Array data)
        {
            // The array from Parquet.Net is typed (e.g., int[] or int?[])
            // We iterate and append.

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