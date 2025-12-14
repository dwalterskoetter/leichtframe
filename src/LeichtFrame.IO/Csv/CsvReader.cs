using System.Globalization;
using System.Text;
using LeichtFrame.Core;

namespace LeichtFrame.IO
{
    /// <summary>
    /// Provides high-performance methods to read CSV files into a <see cref="DataFrame"/>.
    /// Uses parallel processing for full loads and streaming for batched access.
    /// </summary>
    public static class CsvReader
    {
        // =======================================================================
        // STANDARD READ METHODS (Full Load - Parallelized)
        // =======================================================================

        /// <summary>
        /// Reads a CSV file into a DataFrame using parallel processing for maximum speed.
        /// </summary>
        public static DataFrame Read(string path, DataFrameSchema schema, CsvReadOptions? options = null)
        {
            options ??= new CsvReadOptions();

            // We read the file using a StreamReader
            using var reader = new StreamReader(path, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 65536);

            // Estimate capacity (e.g., 10k rows) to reduce resizing
            var df = DataFrame.Create(schema, capacity: 10000);

            // Cache column info for the parallel loop
            int colCount = df.ColumnCount;
            var columns = new IColumn[colCount];
            var colTypes = new Type[colCount];
            for (int i = 0; i < colCount; i++)
            {
                columns[i] = df.Columns[i];
                colTypes[i] = df.Columns[i].DataType;
            }

            // Skip Header
            if (options.HasHeader && !reader.EndOfStream)
            {
                reader.ReadLine();
            }

            char separator = options.Separator[0];

            // Chunk Size: Number of lines to read before spawning parallel tasks.
            // 50,000 is a good balance between memory usage and thread overhead.
            int chunkSize = 50_000;
            var lineBuffer = new List<string>(chunkSize);

            // 1. Read Loop (IO Bound - Single Thread)
            while (!reader.EndOfStream)
            {
                string? line = reader.ReadLine();
                if (line == null) break;

                lineBuffer.Add(line);

                if (lineBuffer.Count >= chunkSize)
                {
                    ProcessChunkParallel(lineBuffer, columns, colTypes, separator, options);
                    lineBuffer.Clear();
                }
            }

            // Process remainder
            if (lineBuffer.Count > 0)
            {
                ProcessChunkParallel(lineBuffer, columns, colTypes, separator, options);
            }

            return df;
        }

        /// <summary>
        /// Reads a CSV from a stream.
        /// Note: Since streams might not be seekable, we copy this to a temp file or read fully if memory allows.
        /// For this implementation, we simply delegate to the parallel logic if it's a FileStream, 
        /// or fall back to a simpler approach if purely in-memory stream to avoid complexity.
        /// </summary>
        public static DataFrame Read(Stream stream, DataFrameSchema schema, CsvReadOptions? options = null)
        {
            if (stream is FileStream fs)
            {
                // Re-open with the optimized path reader
                return Read(fs.Name, schema, options);
            }

            // Fallback for MemoryStream/NetworkStream: Use ReadBatches logic but aggregate into one DF.
            // This reuses the code from ReadBatches (DRY).
            var batches = ReadBatches(stream, schema, batchSize: 50_000, options).ToList();

            if (batches.Count == 0) return DataFrame.Create(schema, 0);
            if (batches.Count == 1) return batches[0];

            return ReadLegacySequential(stream, schema, options);
        }

        /// <summary>
        /// Fallback sequential reader for non-file streams.
        /// </summary>
        private static DataFrame ReadLegacySequential(Stream stream, DataFrameSchema schema, CsvReadOptions? options)
        {
            options ??= new CsvReadOptions();
            // Important: leaveOpen=true because we don't own the stream here
            using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 65536, leaveOpen: true);

            var df = DataFrame.Create(schema, 100);

            var columns = new IColumn[df.ColumnCount];
            for (int i = 0; i < df.ColumnCount; i++) columns[i] = df.Columns[i];

            if (options.HasHeader && !reader.EndOfStream) reader.ReadLine();

            char sep = options.Separator[0];
            string? line;

            while ((line = reader.ReadLine()) != null)
            {
                var parts = SplitCsvLine(line, sep);
                if (parts.Length < columns.Length) continue;

                for (int i = 0; i < columns.Length; i++)
                {
                    ParseAndAppend(columns[i], parts[i], options);
                }
            }
            return df;
        }

        /// <summary>
        /// Reads a CSV file, automatically inferring the schema.
        /// </summary>
        public static DataFrame Read(string path, CsvReadOptions? options = null)
        {
            var schema = InferSchema(path, options);
            return Read(path, schema, options);
        }

        /// <summary>
        /// Reads a CSV file using a POCO class schema.
        /// </summary>
        public static DataFrame Read<T>(string path, CsvReadOptions? options = null)
        {
            var schema = DataFrameSchema.FromType<T>();
            return Read(path, schema, options);
        }

        /// <summary>
        /// Reads a CSV from a stream using a POCO class schema.
        /// </summary>
        public static DataFrame Read<T>(Stream stream, CsvReadOptions? options = null)
        {
            var schema = DataFrameSchema.FromType<T>();
            return Read(stream, schema, options);
        }

        // =======================================================================
        // BATCHED READ METHODS (Streaming)
        // =======================================================================

        /// <summary>
        /// Reads a CSV file in chunks (batches) to enable processing of files larger than memory.
        /// </summary>
        public static IEnumerable<DataFrame> ReadBatches(string path, DataFrameSchema schema, int batchSize = 1000, CsvReadOptions? options = null)
        {
            using var stream = File.OpenRead(path);
            foreach (var batch in ReadBatches(stream, schema, batchSize, options))
            {
                yield return batch;
            }
        }

        /// <summary>
        /// Reads CSV batches from a stream.
        /// </summary>
        public static IEnumerable<DataFrame> ReadBatches(Stream stream, DataFrameSchema schema, int batchSize, CsvReadOptions? options = null)
        {
            if (batchSize <= 0) throw new ArgumentOutOfRangeException(nameof(batchSize), "Batch size must be greater than 0.");

            options ??= new CsvReadOptions();
            using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 65536, leaveOpen: true);

            if (options.HasHeader && !reader.EndOfStream)
            {
                reader.ReadLine();
            }

            char separator = options.Separator[0];
            int colCount = schema.Columns.Count;

            while (!reader.EndOfStream)
            {
                var batchDf = DataFrame.Create(schema, batchSize);
                var batchColumns = new IColumn[colCount];
                for (int i = 0; i < colCount; i++) batchColumns[i] = batchDf[i];

                int rowsInCurrentBatch = 0;

                while (rowsInCurrentBatch < batchSize && !reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    if (line == null) break;

                    var parts = SplitCsvLine(line, separator);

                    if (parts.Length < colCount) continue;

                    for (int i = 0; i < colCount; i++)
                    {
                        ParseAndAppend(batchColumns[i], parts[i], options);
                    }

                    rowsInCurrentBatch++;
                }

                if (rowsInCurrentBatch > 0)
                {
                    yield return batchDf;
                }
            }
        }

        // =======================================================================
        // INTERNAL HELPERS & PARALLEL LOGIC
        // =======================================================================

        private static void ProcessChunkParallel(List<string> lines, IColumn[] columns, Type[] types, char separator, CsvReadOptions options)
        {
            int count = lines.Count;
            int colCount = columns.Length;

            // Temp buffer: [RowIndex][ColIndex]
            var parsedRows = new object?[count][];

            // 1. CPU Bound: Parse in Parallel
            Parallel.For(0, count, rowIdx =>
            {
                string line = lines[rowIdx];
                var parts = SplitCsvLine(line, separator);

                parsedRows[rowIdx] = new object?[colCount];

                if (parts.Length >= colCount)
                {
                    for (int c = 0; c < colCount; c++)
                    {
                        parsedRows[rowIdx][c] = ParseValue(types[c], parts[c], options);
                    }
                }
            });

            // 2. Memory Bound: Write sequentially
            for (int r = 0; r < count; r++)
            {
                var rowData = parsedRows[r];
                if (rowData == null || rowData.Length == 0) continue;

                for (int c = 0; c < colCount; c++)
                {
                    columns[c].AppendObject(rowData[c]);
                }
            }
        }

        private static void ParseAndAppend(IColumn col, string raw, CsvReadOptions options)
        {
            // Re-introduced helper for sequential scenarios
            object? val = ParseValue(col.DataType, raw, options);
            col.AppendObject(val);
        }

        private static object? ParseValue(Type targetType, string raw, CsvReadOptions options)
        {
            if (string.IsNullOrEmpty(raw)) return null;

            if (targetType == typeof(int))
            {
                if (int.TryParse(raw, NumberStyles.Integer, options.Culture, out int result)) return result;
                return null;
            }
            if (targetType == typeof(double))
            {
                if (double.TryParse(raw, NumberStyles.Float | NumberStyles.AllowThousands, options.Culture, out double result)) return result;
                return null;
            }
            if (targetType == typeof(bool))
            {
                if (bool.TryParse(raw, out bool bResult)) return bResult;
                return raw == "1";
            }
            if (targetType == typeof(DateTime))
            {
                if (options.DateFormat != null)
                {
                    if (DateTime.TryParseExact(raw, options.DateFormat, options.Culture, DateTimeStyles.None, out DateTime dtResult))
                        return dtResult;
                }
                else
                {
                    if (DateTime.TryParse(raw, options.Culture, DateTimeStyles.None, out DateTime dtResult))
                        return dtResult;
                }
                return null;
            }
            return raw;
        }

        /// <summary>
        /// Scans the CSV file to infer the schema (column names and types).
        /// </summary>
        public static DataFrameSchema InferSchema(string path, CsvReadOptions? options = null, int sampleRows = 100)
        {
            options ??= new CsvReadOptions();
            using var reader = new StreamReader(File.OpenRead(path));

            string? line = reader.ReadLine();
            if (line == null) throw new IOException("File is empty.");

            char sep = options.Separator[0];
            string[] headers;

            if (options.HasHeader)
            {
                headers = SplitCsvLine(line, sep);
            }
            else
            {
                var firstLineParts = SplitCsvLine(line, sep);
                headers = new string[firstLineParts.Length];
                for (int i = 0; i < headers.Length; i++) headers[i] = $"Column{i}";

                reader.DiscardBufferedData();
                reader.BaseStream.Seek(0, SeekOrigin.Begin);
            }

            var colTypes = new Type[headers.Length];
            var colNullable = new bool[headers.Length];

            int readCount = 0;
            while (readCount < sampleRows && (line = reader.ReadLine()) != null)
            {
                var parts = SplitCsvLine(line, sep);
                if (parts.Length < headers.Length) continue;

                for (int i = 0; i < headers.Length; i++)
                {
                    string val = parts[i];
                    if (string.IsNullOrEmpty(val))
                    {
                        colNullable[i] = true;
                        continue;
                    }
                    Type cellType = DetectType(val, options);
                    colTypes[i] = MergeTypes(colTypes[i], cellType);
                }
                readCount++;
            }

            var definitions = new System.Collections.Generic.List<ColumnDefinition>();
            for (int i = 0; i < headers.Length; i++)
            {
                Type finalType = colTypes[i] ?? typeof(string);
                definitions.Add(new ColumnDefinition(headers[i], finalType, colNullable[i]));
            }

            return new DataFrameSchema(definitions);
        }

        private static string[] SplitCsvLine(string line, char separator)
        {
            var values = new System.Collections.Generic.List<string>();
            int start = 0;
            bool inQuotes = false;

            for (int i = 0; i < line.Length; i++)
            {
                char c = line[i];

                if (c == '\"') inQuotes = !inQuotes;
                else if (c == separator && !inQuotes)
                {
                    values.Add(Unescape(line.Substring(start, i - start)));
                    start = i + 1;
                }
            }

            if (start <= line.Length)
            {
                values.Add(Unescape(line.Substring(start)));
            }

            return values.ToArray();
        }

        private static string Unescape(string value)
        {
            if (string.IsNullOrEmpty(value)) return value;
            if (value.StartsWith("\"") && value.EndsWith("\"") && value.Length >= 2)
            {
                value = value.Substring(1, value.Length - 2);
                return value.Replace("\"\"", "\"");
            }
            return value;
        }

        private static Type DetectType(string val, CsvReadOptions options)
        {
            if (int.TryParse(val, NumberStyles.Integer, options.Culture, out _)) return typeof(int);
            if (double.TryParse(val, NumberStyles.Float | NumberStyles.AllowThousands, options.Culture, out _)) return typeof(double);
            if (bool.TryParse(val, out _)) return typeof(bool);
            if (DateTime.TryParse(val, options.Culture, DateTimeStyles.None, out _)) return typeof(DateTime);
            return typeof(string);
        }

        private static Type MergeTypes(Type? current, Type newType)
        {
            if (current == null) return newType;
            if (current == newType) return current;
            if ((current == typeof(int) && newType == typeof(double)) ||
                (current == typeof(double) && newType == typeof(int)))
                return typeof(double);
            return typeof(string);
        }
    }
}