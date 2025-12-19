using System.Buffers;
using System.Globalization;
using System.Text;
using LeichtFrame.Core;

namespace LeichtFrame.IO
{
    /// <summary>
    /// Provides functionality to read CSV files into DataFrames.
    /// </summary>
    public static class CsvReader
    {
        private static readonly ArrayPool<object?> _objPool = ArrayPool<object?>.Shared;

        // =======================================================================
        // STANDARD READ METHODS
        // =======================================================================

        /// <summary>
        /// Reads a CSV file from the specified path into a DataFrame using the provided schema and options.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="schema"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public static DataFrame Read(string path, DataFrameSchema schema, CsvReadOptions? options = null)
        {
            options ??= new CsvReadOptions();
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 65536);
            return Read(stream, schema, options);
        }

        /// <summary>
        /// Reads a CSV from the provided stream into a DataFrame using the specified schema and options.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="schema"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public static DataFrame Read(Stream stream, DataFrameSchema schema, CsvReadOptions? options = null)
        {
            options ??= new CsvReadOptions();
            using var reader = new StreamReader(stream, Encoding.UTF8, true, 65536, leaveOpen: true);

            var df = DataFrame.Create(schema, 10000);
            int colCount = df.ColumnCount;
            var columns = new IColumn[colCount];
            var colTypes = new Type[colCount];
            var sourceIndices = new int[colCount];

            for (int i = 0; i < colCount; i++)
            {
                columns[i] = df.Columns[i];
                colTypes[i] = df.Columns[i].DataType;
                sourceIndices[i] = schema.Columns[i].SourceIndex ?? i;
            }

            if (options.HasHeader && !reader.EndOfStream) reader.ReadLine();

            char separator = options.Separator[0];
            int chunkSize = 50_000;
            var lineBuffer = new List<string>(chunkSize);

            while (!reader.EndOfStream)
            {
                string? line = reader.ReadLine();
                if (line == null) break;
                lineBuffer.Add(line);

                if (lineBuffer.Count >= chunkSize)
                {
                    ProcessChunkParallel(lineBuffer, columns, colTypes, sourceIndices, separator, options);
                    lineBuffer.Clear();
                }
            }

            if (lineBuffer.Count > 0) ProcessChunkParallel(lineBuffer, columns, colTypes, sourceIndices, separator, options);
            return df;
        }

        /// <summary>
        /// Reads a CSV file from the specified path into a DataFrame by inferring the schema from the data.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="path"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public static DataFrame Read<T>(string path, CsvReadOptions? options = null)
        {
            return Read(path, DataFrameSchema.FromType<T>(), options);
        }

        /// <summary>
        /// Reads a CSV from the provided stream into a DataFrame by inferring the schema from the data.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="stream"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public static DataFrame Read<T>(Stream stream, CsvReadOptions? options = null)
        {
            return Read(stream, DataFrameSchema.FromType<T>(), options);
        }

        // =======================================================================
        // BATCHED READ METHODS (Streaming)
        // =======================================================================

        /// <summary>
        /// Reads a CSV file from the specified path into multiple DataFrame batches using the provided schema and options.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="schema"></param>
        /// <param name="batchSize"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public static IEnumerable<DataFrame> ReadBatches(string path, DataFrameSchema schema, int batchSize = 1000, CsvReadOptions? options = null)
        {
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 65536);
            foreach (var batch in ReadBatches(stream, schema, batchSize, options)) yield return batch;
        }

        /// <summary>
        /// Reads a CSV from the provided stream into multiple DataFrame batches using the specified schema and options.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="schema"></param>
        /// <param name="batchSize"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public static IEnumerable<DataFrame> ReadBatches(Stream stream, DataFrameSchema schema, int batchSize, CsvReadOptions? options = null)
        {
            options ??= new CsvReadOptions();
            using var reader = new StreamReader(stream, Encoding.UTF8, true, 65536, leaveOpen: true);
            if (options.HasHeader && !reader.EndOfStream) reader.ReadLine();

            char separator = options.Separator[0];
            int colCount = schema.Columns.Count;
            int[] sourceIndices = schema.Columns.Select((c, i) => c.SourceIndex ?? i).ToArray();

            while (!reader.EndOfStream)
            {
                var batchDf = DataFrame.Create(schema, batchSize);
                for (int r = 0; r < batchSize && !reader.EndOfStream; r++)
                {
                    string? line = reader.ReadLine();
                    if (line == null) break;

                    ReadOnlySpan<char> lineSpan = line.AsSpan();
                    for (int i = 0; i < colCount; i++)
                    {
                        var field = GetFieldAt(lineSpan, separator, sourceIndices[i]);
                        batchDf[i].AppendObject(ParseValueFromSpan(schema.Columns[i].DataType, field, options));
                    }
                }
                if (batchDf.RowCount > 0) yield return batchDf;
            }
        }

        // =======================================================================
        // SCHEMA INFERENCE
        // =======================================================================

        /// <summary>
        /// Infers the schema of a CSV file by sampling a specified number of rows.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="options"></param>
        /// <param name="sampleRows"></param>
        /// <returns></returns>
        /// <exception cref="IOException"></exception>
        public static DataFrameSchema InferSchema(string path, CsvReadOptions? options = null, int sampleRows = 100)
        {
            options ??= new CsvReadOptions();
            using var reader = new StreamReader(path, Encoding.UTF8, true, 65536);

            string? firstLine = reader.ReadLine();
            if (firstLine == null) throw new IOException("File is empty.");

            char sep = options.Separator[0];
            string[] headers;
            if (options.HasHeader)
            {
                headers = firstLine.Split(sep).Select(h => h.Trim('\"', ' ')).ToArray();
            }
            else
            {
                headers = firstLine.Split(sep).Select((_, i) => $"Column{i}").ToArray();
                reader.BaseStream.Seek(0, SeekOrigin.Begin);
                reader.DiscardBufferedData();
            }

            var colTypes = new Type?[headers.Length];
            var colNullable = new bool[headers.Length];

            for (int r = 0; r < sampleRows && !reader.EndOfStream; r++)
            {
                string? line = reader.ReadLine();
                if (line == null) break;
                if (string.IsNullOrWhiteSpace(line))
                {
                    for (int i = 0; i < headers.Length; i++) colNullable[i] = true;
                    continue;
                }

                ReadOnlySpan<char> lineSpan = line.AsSpan();
                for (int i = 0; i < headers.Length; i++)
                {
                    var field = GetFieldAt(lineSpan, sep, i);
                    if (field.IsEmpty || field.IsWhiteSpace())
                    {
                        colNullable[i] = true;
                        continue;
                    }
                    Type detected = DetectTypeFromSpan(field, options);
                    colTypes[i] = MergeTypes(colTypes[i], detected);
                }
            }

            return new DataFrameSchema(headers.Select((h, i) =>
                new ColumnDefinition(h, colTypes[i] ?? typeof(string), colNullable[i], SourceIndex: i)).ToList());
        }

        // =======================================================================
        // INTERNAL HELPERS
        // =======================================================================

        private static void ProcessChunkParallel(List<string> lines, IColumn[] columns, Type[] types, int[] sourceIndices, char separator, CsvReadOptions options)
        {
            int rowCount = lines.Count;
            int colCount = columns.Length;
            object?[] results = _objPool.Rent(rowCount * colCount);

            Parallel.For(0, rowCount, r =>
            {
                ReadOnlySpan<char> lineSpan = lines[r].AsSpan();
                for (int c = 0; c < colCount; c++)
                {
                    var field = GetFieldAt(lineSpan, separator, sourceIndices[c]);
                    results[r * colCount + c] = ParseValueFromSpan(types[c], field, options);
                }
            });

            for (int r = 0; r < rowCount; r++)
                for (int c = 0; c < colCount; c++)
                    columns[c].AppendObject(results[r * colCount + c]);

            _objPool.Return(results);
        }

        private static ReadOnlySpan<char> GetFieldAt(ReadOnlySpan<char> line, char separator, int targetIndex)
        {
            int currentIdx = 0;
            int start = 0;
            bool inQuotes = false;

            for (int i = 0; i < line.Length; i++)
            {
                if (line[i] == '\"') inQuotes = !inQuotes;
                else if (line[i] == separator && !inQuotes)
                {
                    if (currentIdx == targetIndex) return Unescape(line.Slice(start, i - start));
                    start = i + 1;
                    currentIdx++;
                }
            }
            return (currentIdx == targetIndex) ? Unescape(line.Slice(start)) : ReadOnlySpan<char>.Empty;
        }

        private static ReadOnlySpan<char> Unescape(ReadOnlySpan<char> s)
        {
            s = s.Trim();
            return (s.Length >= 2 && s[0] == '\"' && s[^1] == '\"') ? s.Slice(1, s.Length - 2) : s;
        }

        private static object? ParseValueFromSpan(Type type, ReadOnlySpan<char> span, CsvReadOptions options)
        {
            if (span.IsEmpty || span.IsWhiteSpace()) return null;
            if (type == typeof(int) && int.TryParse(span, NumberStyles.Integer, options.Culture, out int i)) return i;
            if (type == typeof(double) && double.TryParse(span, NumberStyles.Float | NumberStyles.AllowThousands, options.Culture, out double d)) return d;
            if (type == typeof(DateTime) && DateTime.TryParse(span, options.Culture, DateTimeStyles.None, out DateTime dt)) return dt;
            if (type == typeof(bool) && bool.TryParse(span, out bool b)) return b;
            return (type == typeof(string)) ? span.ToString() : null;
        }

        private static Type DetectTypeFromSpan(ReadOnlySpan<char> span, CsvReadOptions options)
        {
            if (int.TryParse(span, NumberStyles.Integer, options.Culture, out _)) return typeof(int);
            if (double.TryParse(span, NumberStyles.Float | NumberStyles.AllowThousands, options.Culture, out _)) return typeof(double);
            if (DateTime.TryParse(span, options.Culture, DateTimeStyles.None, out _)) return typeof(DateTime);
            if (bool.TryParse(span, out _)) return typeof(bool);
            return typeof(string);
        }

        private static Type MergeTypes(Type? current, Type newType)
        {
            if (current == null || current == newType) return newType;
            if ((current == typeof(int) && newType == typeof(double)) || (current == typeof(double) && newType == typeof(int))) return typeof(double);
            return typeof(string);
        }
    }
}