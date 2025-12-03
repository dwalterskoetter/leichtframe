using Apache.Arrow; // NuGet Namespace
using LeichtFrame.Core;

namespace LeichtFrame.IO
{
    public static class ArrowConverter
    {
        /// <summary>
        /// Converts an Apache Arrow RecordBatch into a LeichtFrame DataFrame.
        /// Performs a deep copy of the data (Phase 1 implementation).
        /// </summary>
        public static DataFrame ToDataFrame(RecordBatch batch)
        {
            if (batch == null) throw new ArgumentNullException(nameof(batch));

            var columns = new List<IColumn>(batch.ColumnCount);
            int rowCount = batch.Length;

            // Iterate over Arrow Arrays (Columns)
            foreach (var field in batch.Schema.FieldsList)
            {
                var arrowArray = batch.Column(field.Name);

                // We use the name from the schema
                string name = field.Name;

                // Conversion based on the Arrow type
                IColumn lfCol = ConvertArray(name, arrowArray, rowCount);
                columns.Add(lfCol);
            }

            return new DataFrame(columns);
        }

        private static IColumn ConvertArray(string name, IArrowArray array, int length)
        {
            // 1. Int32
            if (array is Int32Array intArray)
            {
                var col = new IntColumn(name, length, isNullable: true); // Arrow is usually nullable
                for (int i = 0; i < length; i++)
                {
                    if (intArray.IsNull(i)) col.Append(null);
                    else col.Append(intArray.GetValue(i));
                }
                return col;
            }

            // 2. Double
            if (array is DoubleArray doubleArray)
            {
                var col = new DoubleColumn(name, length, isNullable: true);
                for (int i = 0; i < length; i++)
                {
                    if (doubleArray.IsNull(i)) col.Append(null);
                    else col.Append(doubleArray.GetValue(i));
                }
                return col;
            }

            // 3. String
            if (array is StringArray stringArray)
            {
                var col = new StringColumn(name, length, isNullable: true);
                for (int i = 0; i < length; i++)
                {
                    // GetString returns null if null
                    col.Append(stringArray.GetString(i));
                }
                return col;
            }

            // 4. Bool
            if (array is BooleanArray boolArray)
            {
                var col = new BoolColumn(name, length, isNullable: true);
                for (int i = 0; i < length; i++)
                {
                    if (boolArray.IsNull(i)) col.Append(null);
                    else col.Append(boolArray.GetValue(i));
                }
                return col;
            }

            // 5. Date/Timestamp (Arrow has many time types, we support basic Timestamp here)
            if (array is TimestampArray tsArray)
            {
                var col = new DateTimeColumn(name, length, isNullable: true);
                for (int i = 0; i < length; i++)
                {
                    if (tsArray.IsNull(i)) col.Append(null);
                    else
                    {
                        // Arrow Timestamp is usually DateTimeOffset, we take DateTime
                        col.Append(tsArray.GetTimestamp(i)?.DateTime);
                    }
                }
                return col;
            }

            // Fallback for Date32 (Common in Parquet/Arrow)
            if (array is Date32Array date32Array)
            {
                var col = new DateTimeColumn(name, length, isNullable: true);
                for (int i = 0; i < length; i++)
                {
                    if (date32Array.IsNull(i)) col.Append(null);
                    else col.Append(date32Array.GetDateTime(i));
                }
                return col;
            }

            throw new NotSupportedException($"Arrow array type '{array.GetType().Name}' is not supported yet.");
        }
    }
}