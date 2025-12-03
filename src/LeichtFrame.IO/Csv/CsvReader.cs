using LeichtFrame.Core;

namespace LeichtFrame.IO
{
    public static class CsvReader
    {
        public static DataFrame Read(string path, DataFrameSchema schema, CsvReadOptions? options = null)
        {
            using var stream = File.OpenRead(path);
            return Read(stream, schema, options);
        }

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
    }
}