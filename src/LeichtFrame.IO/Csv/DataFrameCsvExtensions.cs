using System.Globalization;
using System.Text;
using LeichtFrame.Core;

namespace LeichtFrame.IO
{
    public static class DataFrameCsvExtensions
    {
        // =========================================================
        // WRITE EXTENSIONS (Export)
        // =========================================================

        /// <summary>
        /// Writes the DataFrame to a CSV file.
        /// </summary>
        public static void WriteCsv(this DataFrame df, string path, CsvWriteOptions? options = null)
        {
            CsvWriter.Write(df, path, options);
        }

        /// <summary>
        /// Writes the DataFrame to a stream in CSV format.
        /// </summary>
        public static void WriteCsv(this DataFrame df, Stream stream, CsvWriteOptions? options = null)
        {
            CsvWriter.Write(df, stream, options);
        }

        // =========================================================
        // READ EXTENSIONS (Import)
        // =========================================================

        /// <summary>
        /// Reads a CSV file from a given path.
        /// </summary>
        public static DataFrame ReadCsv(string path, DataFrameSchema schema, bool hasHeader = true, char separator = ',')
        {
            using var stream = File.OpenRead(path);
            return ReadCsv(stream, schema, hasHeader, separator);
        }

        /// <summary>
        /// Reads a CSV from a stream (e.g. MemoryStream or FileStream).
        /// </summary>
        public static DataFrame ReadCsv(Stream stream, DataFrameSchema schema, bool hasHeader = true, char separator = ',')
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            if (schema == null) throw new ArgumentNullException(nameof(schema));

            // Estimate row count roughly to minimize resizing (Performance optimization).
            // Assumption: approx. 100 bytes per row. Better than starting at capacity 0.
            int estimatedRows = (int)(stream.Length / 100);
            if (estimatedRows < 16) estimatedRows = 16;

            // 1. Create DataFrame (with schema and estimated capacity).
            var df = DataFrame.Create(schema, estimatedRows);

            using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 65536);

            // 2. Skip Header if present
            if (hasHeader && !reader.EndOfStream)
            {
                reader.ReadLine();
            }

            // Cache columns to avoid dictionary lookup per row
            var columns = df.Columns;
            int colCount = columns.Count;

            // 3. Read and parse lines
            while (!reader.EndOfStream)
            {
                var line = reader.ReadLine();
                if (string.IsNullOrWhiteSpace(line)) continue;

                var parts = line.Split(separator);

                // Strict schema check: Ignore lines with fewer columns than schema
                if (parts.Length < colCount) continue;

                for (int i = 0; i < colCount; i++)
                {
                    string rawValue = parts[i];
                    var col = columns[i];

                    try
                    {
                        ParseAndAppend(col, rawValue);
                    }
                    catch
                    {
                        // Fallback on parse error: Append null or default
                        if (col.IsNullable)
                        {
                            col.AppendObject(null);
                        }
                        else
                        {
                            col.AppendObject(GetDefault(col.DataType));
                        }
                    }
                }
            }

            return df;
        }

        // =========================================================
        // INTERNAL HELPERS
        // =========================================================

        private static void ParseAndAppend(IColumn col, string rawValue)
        {
            Type targetType = col.DataType;

            // Handle Nulls
            if (string.IsNullOrEmpty(rawValue))
            {
                if (col.IsNullable)
                {
                    col.AppendObject(null);
                    return;
                }
            }

            // Parsing Logic (Always use InvariantCulture for data interchange!)
            if (targetType == typeof(int))
            {
                if (int.TryParse(rawValue, NumberStyles.Any, CultureInfo.InvariantCulture, out int result))
                    col.AppendObject(result);
                else
                    col.AppendObject(GetDefault(typeof(int))); // Fallback
            }
            else if (targetType == typeof(double))
            {
                if (double.TryParse(rawValue, NumberStyles.Any, CultureInfo.InvariantCulture, out double result))
                    col.AppendObject(result);
                else
                    col.AppendObject(GetDefault(typeof(double)));
            }
            else if (targetType == typeof(bool))
            {
                if (bool.TryParse(rawValue, out bool result))
                    col.AppendObject(result);
                else
                    col.AppendObject(false);
            }
            else if (targetType == typeof(string))
            {
                col.AppendObject(rawValue);
            }
            else if (targetType == typeof(DateTime))
            {
                if (DateTime.TryParse(rawValue, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime result))
                    col.AppendObject(result);
                else
                    col.AppendObject(GetDefault(typeof(DateTime)));
            }
            else
            {
                // General Fallback
                try
                {
                    col.AppendObject(Convert.ChangeType(rawValue, targetType, CultureInfo.InvariantCulture));
                }
                catch
                {
                    col.AppendObject(GetDefault(targetType));
                }
            }
        }

        private static object? GetDefault(Type type)
        {
            if (type.IsValueType)
            {
                return Activator.CreateInstance(type);
            }
            return null;
        }
    }
}