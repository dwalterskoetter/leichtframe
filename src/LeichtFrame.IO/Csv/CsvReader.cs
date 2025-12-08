using System.Globalization;
using LeichtFrame.Core;

namespace LeichtFrame.IO
{
    /// <summary>
    /// Provides high-performance methods to read CSV files into a <see cref="DataFrame"/>.
    /// Supports automatic schema inference and strongly-typed POCO mapping.
    /// </summary>
    public static class CsvReader
    {
        /// <summary>
        /// Reads a CSV file using a manually defined schema.
        /// Use this if you want to define columns dynamically at runtime.
        /// </summary>
        /// <param name="path">The file path to the CSV.</param>
        /// <param name="schema">The schema definition containing column names and types.</param>
        /// <param name="options">Optional CSV parsing options (separator, culture).</param>
        /// <returns>A populated <see cref="DataFrame"/>.</returns>
        public static DataFrame Read(string path, DataFrameSchema schema, CsvReadOptions? options = null)
        {
            using var stream = File.OpenRead(path);
            return Read(stream, schema, options);
        }


        /// <summary>
        /// Reads a CSV from a stream using a manually defined schema.
        /// </summary>
        /// <param name="stream">The input stream containing CSV data.</param>
        /// <param name="schema">The schema definition containing column names and types.</param>
        /// <param name="options">Optional CSV parsing options.</param>
        /// <returns>A populated <see cref="DataFrame"/>.</returns>
        public static DataFrame Read(Stream stream, DataFrameSchema schema, CsvReadOptions? options = null)
        {
            options ??= new CsvReadOptions();
            using var reader = new StreamReader(stream);

            // 1. Setup DataFrame
            var df = DataFrame.Create(schema, capacity: 100);

            // Cache Columns
            var columns = new IColumn[df.ColumnCount];
            for (int i = 0; i < df.ColumnCount; i++)
            {
                columns[i] = df.Columns[i];
            }

            // 2. Header Handling
            if (options.HasHeader)
            {
                reader.ReadLine();
            }

            // 3. Read Loop
            string? line;
            char[] sep = options.Separator.ToCharArray();

            while ((line = reader.ReadLine()) != null)
            {
                // FIX: Statt line.Split(sep) nutzen wir unseren Smart Splitter
                var parts = SplitCsvLine(line, options.Separator[0]); // Annahme: Separator ist 1 Char

                if (parts.Length < columns.Length) continue;

                for (int i = 0; i < columns.Length; i++)
                {
                    ParseAndAppend(columns[i], parts[i], options);
                }
            }

            return df;
        }

        private static void ParseAndAppend(IColumn col, string raw, CsvReadOptions options)
        {
            // Trim whitespace for numbers/dates is usually safer
            // raw = raw.Trim(); // Optional, depends on strictness

            // Handle Nulls
            if (string.IsNullOrEmpty(raw))
            {
                if (col is IntColumn ic) ic.Append(null);
                else if (col is DoubleColumn dc) dc.Append(null);
                else if (col is StringColumn sc) sc.Append(null);
                else if (col is BoolColumn bc) bc.Append(null);
                else if (col is DateTimeColumn dtc) dtc.Append(null);
                else throw new NotSupportedException($"Unknown column type: {col.GetType().Name}");
                return;
            }

            // Parse Values
            if (col is IntColumn iCol)
            {
                iCol.Append(int.Parse(raw, options.Culture));
            }
            else if (col is DoubleColumn dCol)
            {
                dCol.Append(double.Parse(raw, options.Culture));
            }
            else if (col is StringColumn sCol)
            {
                sCol.Append(raw);
            }
            else if (col is BoolColumn bCol)
            {
                if (bool.TryParse(raw, out bool bResult)) bCol.Append(bResult);
                else bCol.Append(raw == "1");
            }
            else if (col is DateTimeColumn dtCol)
            {
                if (options.DateFormat != null)
                    dtCol.Append(DateTime.ParseExact(raw, options.DateFormat, options.Culture));
                else
                    dtCol.Append(DateTime.Parse(raw, options.Culture));
            }
        }

        /// <summary>
        /// Splits a CSV line respecting quotes (RFC 4180).
        /// </summary>
        private static string[] SplitCsvLine(string line, char separator)
        {
            var values = new System.Collections.Generic.List<string>();
            int start = 0;
            bool inQuotes = false;

            for (int i = 0; i < line.Length; i++)
            {
                char c = line[i];

                if (c == '\"')
                {
                    inQuotes = !inQuotes; // Toggle Status
                }
                else if (c == separator && !inQuotes)
                {
                    // Trennzeichen gefunden (und nicht innerhalb von Quotes)
                    values.Add(Unescape(line.Substring(start, i - start)));
                    start = i + 1;
                }
            }

            // Letzten Wert hinzufügen
            if (start <= line.Length)
            {
                values.Add(Unescape(line.Substring(start)));
            }

            return values.ToArray();
        }

        /// <summary>
        /// Removes surrounding quotes and handles double-quote escaping.
        /// </summary>
        private static string Unescape(string value)
        {
            if (string.IsNullOrEmpty(value)) return value;

            // 1. Anführungszeichen entfernen, falls vorhanden
            if (value.StartsWith("\"") && value.EndsWith("\"") && value.Length >= 2)
            {
                value = value.Substring(1, value.Length - 2);

                // 2. Doppelte Quotes ("") zu einem (") machen
                return value.Replace("\"\"", "\"");
            }

            return value;
        }

        /// <summary>
        /// Scans the CSV file to infer the schema (column names and types).
        /// </summary>
        /// <param name="path">Path to the CSV file.</param>
        /// <param name="options">Read options (separator, culture).</param>
        /// <param name="sampleRows">Number of rows to scan for type detection.</param>
        /// <returns>The inferred DataFrameSchema.</returns>
        public static DataFrameSchema InferSchema(string path, CsvReadOptions? options = null, int sampleRows = 100)
        {
            options ??= new CsvReadOptions();
            using var reader = new StreamReader(File.OpenRead(path));

            string? line = reader.ReadLine();
            if (line == null) throw new IOException("File is empty.");

            // 1. Determine Column Names
            string[] headers;
            char sep = options.Separator[0]; // MVP assumption: 1 char separator

            if (options.HasHeader)
            {
                headers = SplitCsvLine(line, sep);
            }
            else
            {
                var firstLineParts = SplitCsvLine(line, sep);
                headers = new string[firstLineParts.Length];
                for (int i = 0; i < headers.Length; i++) headers[i] = $"Column{i}";

                // Reset stream required? StreamReader is forward only.
                // If no header, the first line IS data. We must restart or parse first line manually.
                // For simplicity: close and reopen or use BaseStream.Seek if FileStream.
                // Easier for file path: Re-open reader or extract logic to scan lines.

                // FIX: Wenn kein Header, müssen wir die Datei neu lesen, um Zeile 1 auch als Daten zu scannen.
                // Da wir StreamReader nicht resetten können (außer BaseStream.Position = 0), schließen wir kurz.
                reader.DiscardBufferedData();
                reader.BaseStream.Seek(0, SeekOrigin.Begin);
            }

            // 2. Init Types (Assumption: Everything is Unknown/Int initially)
            var colTypes = new Type[headers.Length];
            var colNullable = new bool[headers.Length];

            // Wir lesen N Zeilen
            int readCount = 0;
            while (readCount < sampleRows && (line = reader.ReadLine()) != null)
            {
                var parts = SplitCsvLine(line, sep);

                // Handle ragged lines (skip or adjust) - MVP: ignore short lines
                if (parts.Length < headers.Length) continue;

                for (int i = 0; i < headers.Length; i++)
                {
                    string val = parts[i];

                    if (string.IsNullOrEmpty(val))
                    {
                        colNullable[i] = true;
                        continue;
                    }

                    // Determine type of this specific cell
                    Type cellType = DetectType(val, options);

                    // Merge with existing column type (Type Promotion)
                    colTypes[i] = MergeTypes(colTypes[i], cellType);
                }
                readCount++;
            }

            // 3. Build Schema
            var definitions = new System.Collections.Generic.List<ColumnDefinition>();
            for (int i = 0; i < headers.Length; i++)
            {
                // Fallback: If all were null or empty -> String
                Type finalType = colTypes[i] ?? typeof(string);
                definitions.Add(new ColumnDefinition(headers[i], finalType, colNullable[i]));
            }

            return new DataFrameSchema(definitions);
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

            // Type Promotion Rules

            // Int + Double -> Double
            if ((current == typeof(int) && newType == typeof(double)) ||
                (current == typeof(double) && newType == typeof(int)))
                return typeof(double);

            // Anything else mixed -> String (Safety fallback)
            // Example: Bool + Int -> String ("True", "123")
            // Example: Date + Double -> String
            return typeof(string);
        }

        /// <summary>
        /// Reads a CSV file, automatically inferring the schema from the header and content.
        /// </summary>
        public static DataFrame Read(string path, CsvReadOptions? options = null)
        {
            // 1. Schema raten
            var schema = InferSchema(path, options);

            // 2. Mit dem geratenen Schema lesen
            return Read(path, schema, options);
        }

        /// <summary>
        /// Reads a CSV file using a POCO class to define the schema strongly typed.
        /// </summary>
        public static DataFrame Read<T>(string path, CsvReadOptions? options = null)
        {
            var schema = DataFrameSchema.FromType<T>();
            return Read(path, schema, options);
        }

        /// <summary>
        /// Reads a CSV from a stream using a POCO class schema.
        /// </summary>
        /// <typeparam name="T">The POCO class defining the schema.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="options">Optional CSV parsing options.</param>
        public static DataFrame Read<T>(Stream stream, CsvReadOptions? options = null)
        {
            var schema = DataFrameSchema.FromType<T>();
            return Read(stream, schema, options);
        }
    }
}