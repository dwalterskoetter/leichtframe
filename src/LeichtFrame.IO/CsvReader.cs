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
                // Simple Split (Warning: No support for quotes/escaping in MVP!)
                var parts = line.Split(sep);

                // Robustness: If line is too short, ignore or error? 
                // MVP: We assume CSV is valid or crash on IndexOutOfRange.
                // Better Fail-Fast:
                if (parts.Length < columns.Length) continue; // Skip empty/broken lines

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
    }
}