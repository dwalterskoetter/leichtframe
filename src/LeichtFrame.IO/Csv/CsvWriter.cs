using System.Text;
using LeichtFrame.Core;

namespace LeichtFrame.IO
{
    /// <summary>
    /// Provides methods for writing <see cref="DataFrame"/> content to CSV format.
    /// Handles proper escaping (RFC 4180) and formatting based on configurable options.
    /// </summary>
    public static class CsvWriter
    {
        /// <summary>
        /// Writes the DataFrame to a CSV file at the specified path.
        /// Overwrites the file if it already exists.
        /// </summary>
        /// <param name="df">The DataFrame to write.</param>
        /// <param name="path">The full file path.</param>
        /// <param name="options">Optional formatting options (separator, date format, etc.).</param>
        public static void Write(DataFrame df, string path, CsvWriteOptions? options = null)
        {
            // File.Create overrides existing files
            using var stream = File.Create(path);
            Write(df, stream, options);
        }

        /// <summary>
        /// Writes the DataFrame to a stream in CSV format.
        /// </summary>
        /// <param name="df">The DataFrame to write.</param>
        /// <param name="stream">The output stream (must be writable).</param>
        /// <param name="options">Optional formatting options.</param>
        public static void Write(DataFrame df, Stream stream, CsvWriteOptions? options = null)
        {
            options ??= new CsvWriteOptions();

            // UTF8 without BOM is the standard nowadays, let the stream decide or enforce it
            using var writer = new StreamWriter(stream, new UTF8Encoding(false), 1024, leaveOpen: true);

            // 1. Write Header
            if (options.WriteHeader)
            {
                // Uses the API from B.5.2
                var headers = df.GetColumnNames();
                writer.WriteLine(string.Join(options.Separator, headers));
            }

            // 2. Write Rows
            var sb = new StringBuilder();

            for (int i = 0; i < df.RowCount; i++)
            {
                sb.Clear();
                for (int c = 0; c < df.ColumnCount; c++)
                {
                    if (c > 0) sb.Append(options.Separator);

                    var col = df[c];
                    // Untyped access (GetValue) is okay here since IO is the bottleneck anyway
                    object? val = col.GetValue(i);

                    string valStr = FormatValue(val, options);

                    // CSV Escaping (RFC 4180): 
                    // If separator, quote, or newline are present -> enclose text in quotes
                    if (NeedsEscaping(valStr, options.Separator))
                    {
                        // Escape double quotes (" -> "")
                        valStr = "\"" + valStr.Replace("\"", "\"\"") + "\"";
                    }

                    sb.Append(valStr);
                }
                writer.WriteLine(sb.ToString());
            }

            writer.Flush();
        }

        private static bool NeedsEscaping(string val, string separator)
        {
            // Performance check: Contains any of the critical characters?
            return val.Contains(separator) || val.Contains("\"") || val.Contains("\n") || val.Contains("\r");
        }

        private static string FormatValue(object? val, CsvWriteOptions options)
        {
            if (val == null) return options.NullValue;

            if (val is DateTime dt)
            {
                return dt.ToString(options.DateFormat, options.Culture);
            }
            if (val is bool b)
            {
                // Lowercase (true/false) or C# standard (True/False)? 
                // C# ToString() produces "True", JSON/JS prefers "true". We stick to C# standard for consistency.
                return b.ToString(options.Culture);
            }
            if (val is IFormattable formattable)
            {
                return formattable.ToString(null, options.Culture);
            }

            return val.ToString() ?? "";
        }
    }
}