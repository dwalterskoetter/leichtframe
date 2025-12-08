using Apache.Arrow;
using Apache.Arrow.Types;
using LeichtFrame.Core;

namespace LeichtFrame.IO
{
    /// <summary>
    /// Provides interoperability methods to convert between LeichtFrame <see cref="DataFrame"/> 
    /// and Apache Arrow <see cref="RecordBatch"/>.
    /// Enables integration with the broader data ecosystem (Spark, Python, etc.).
    /// </summary>
    public static class ArrowConverter
    {
        /// <summary>
        /// Converts an Apache Arrow RecordBatch into a LeichtFrame DataFrame.
        /// <para>
        /// **Note:** Currently performs a deep copy of the data.
        /// Zero-copy integration is planned for future releases.
        /// </para>
        /// </summary>
        /// <param name="batch">The source Apache Arrow RecordBatch.</param>
        /// <returns>A new <see cref="DataFrame"/> containing the data from the RecordBatch.</returns>
        /// <exception cref="ArgumentNullException">Thrown if the batch is null.</exception>
        /// <exception cref="NotSupportedException">Thrown if the Arrow data type is not supported by LeichtFrame.</exception>
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

        /// <summary>
        /// Converts a LeichtFrame DataFrame into an Apache Arrow RecordBatch.
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <returns>A new <see cref="RecordBatch"/> containing the data.</returns>
        /// <exception cref="ArgumentNullException">Thrown if the DataFrame is null.</exception>
        /// <exception cref="NotSupportedException">Thrown if a column type cannot be mapped to Arrow.</exception>
        public static RecordBatch ToRecordBatch(DataFrame df)
        {
            if (df == null) throw new ArgumentNullException(nameof(df));

            // 1. Build Arrow Schema
            var builder = new Schema.Builder();
            foreach (var col in df.Columns)
            {
                builder.Field(f => f.Name(col.Name).DataType(GetArrowType(col.DataType)).Nullable(col.IsNullable));
            }
            var arrowSchema = builder.Build();

            // 2. Build Arrow Arrays
            var arrowArrays = new List<IArrowArray>(df.ColumnCount);
            foreach (var col in df.Columns)
            {
                arrowArrays.Add(BuildArrowArray(col));
            }

            // 3. Create Batch
            return new RecordBatch(arrowSchema, arrowArrays, df.RowCount);
        }

        private static IArrowType GetArrowType(Type type)
        {
            if (type == typeof(int)) return Int32Type.Default;
            if (type == typeof(double)) return DoubleType.Default;
            if (type == typeof(bool)) return BooleanType.Default;
            if (type == typeof(string)) return StringType.Default;
            if (type == typeof(DateTime)) return TimestampType.Default;

            throw new NotSupportedException($"Type '{type.Name}' cannot be mapped to Arrow.");
        }

        private static IArrowArray BuildArrowArray(IColumn col)
        {
            if (col is IntColumn ic)
            {
                var builder = new Int32Array.Builder();
                for (int i = 0; i < ic.Length; i++)
                {
                    if (ic.IsNull(i)) builder.AppendNull();
                    else builder.Append(ic.Get(i));
                }
                return builder.Build();
            }

            if (col is DoubleColumn dc)
            {
                var builder = new DoubleArray.Builder();
                for (int i = 0; i < dc.Length; i++)
                {
                    if (dc.IsNull(i)) builder.AppendNull();
                    else builder.Append(dc.Get(i));
                }
                return builder.Build();
            }

            if (col is StringColumn sc)
            {
                var builder = new StringArray.Builder();
                for (int i = 0; i < sc.Length; i++)
                {
                    // StringColumn handles nulls internally in Get() usually, but checking IsNull is safer/consistent
                    if (sc.IsNull(i)) builder.AppendNull();
                    else builder.Append(sc.Get(i));
                }
                return builder.Build();
            }

            if (col is BoolColumn bc)
            {
                var builder = new BooleanArray.Builder();
                for (int i = 0; i < bc.Length; i++)
                {
                    if (bc.IsNull(i)) builder.AppendNull();
                    else builder.Append(bc.Get(i));
                }
                return builder.Build();
            }

            if (col is DateTimeColumn dtc)
            {
                var builder = new TimestampArray.Builder();
                for (int i = 0; i < dtc.Length; i++)
                {
                    if (dtc.IsNull(i)) builder.AppendNull();
                    else
                    {
                        // Arrow prefers DateTimeOffset usually, but creates TimestampArray from it.
                        builder.Append(new DateTimeOffset(dtc.Get(i)));
                    }
                }
                return builder.Build();
            }

            throw new NotSupportedException($"Column type '{col.GetType().Name}' is not supported for Arrow export.");
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