using LeichtFrame.Core;
using Parquet.Data;
using Parquet.Schema;

namespace LeichtFrame.IO
{
    /// <summary>
    /// Provides methods for writing <see cref="DataFrame"/> objects into Apache Parquet format.
    /// Handles schema mapping and efficient data conversion for storage.
    /// </summary>
    public static class ParquetWriter
    {
        /// <summary>
        /// Writes the DataFrame to a Parquet file at the specified path.
        /// If the file exists, it will be overwritten.
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="path">The output file path.</param>
        public static void Write(DataFrame df, string path)
        {
            // Allow overwrite
            using var stream = File.Create(path);
            Write(df, stream);
        }

        /// <summary>
        /// Writes the DataFrame to a stream in Parquet format synchronously.
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="stream">The writable output stream.</param>
        public static void Write(DataFrame df, Stream stream)
        {
            // Synchronous Wrapper
            WriteAsync(df, stream).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Writes the DataFrame to a stream in Parquet format asynchronously.
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="stream">The writable output stream.</param>
        /// <returns>A task representing the asynchronous write operation.</returns>
        public static async Task WriteAsync(DataFrame df, Stream stream)
        {
            // 1. Schema Mapping (LeichtFrame -> Parquet)
            var dataFields = df.Schema.Columns.Select(MapToDataField).ToArray();
            var parquetSchema = new ParquetSchema(dataFields);

            // 2. Writer Setup
            using var writer = await Parquet.ParquetWriter.CreateAsync(parquetSchema, stream);

            // We write everything in one RowGroup (simplest solution for MVP)
            using var groupWriter = writer.CreateRowGroup();

            // 3. Column Data Conversion & Write
            for (int i = 0; i < df.ColumnCount; i++)
            {
                var col = df.Columns[i];
                var field = dataFields[i];

                // Data conversion (NullBitmap -> Nullable Array)
                Array data = ConvertToParquetArray(col);

                var dataColumn = new DataColumn(field, data);
                await groupWriter.WriteColumnAsync(dataColumn);
            }
        }

        private static DataField MapToDataField(ColumnDefinition def)
        {
            // Int -> Int32, Nullable handling via Type?
            Type t = def.DataType;
            if (def.IsNullable && t.IsValueType)
            {
                t = typeof(Nullable<>).MakeGenericType(t);
            }
            return new DataField(def.Name, t);
        }

        private static Array ConvertToParquetArray(IColumn col)
        {
            // Fast Path: If not nullable and primitive, we might be able to use the array directly?
            // Unfortunately, Values.Span does not return the array, and Parquet.Net requires an Array.
            // We usually have to copy to be safe (snapshot).

            if (col is IntColumn ic)
            {
                if (!ic.IsNullable) return ic.Values.ToArray(); // int[]

                // Nullable Conversion: int[] + bitmap -> int?[]
                var result = new int?[ic.Length];
                for (int i = 0; i < ic.Length; i++)
                    result[i] = ic.IsNull(i) ? null : ic.Get(i);
                return result;
            }

            if (col is DoubleColumn dc)
            {
                if (!dc.IsNullable) return dc.Values.ToArray();

                var result = new double?[dc.Length];
                for (int i = 0; i < dc.Length; i++)
                    result[i] = dc.IsNull(i) ? null : dc.Get(i);
                return result;
            }

            if (col is BoolColumn bc)
            {
                // BoolColumn is bit-packed internally. We need to unpack to bool[] or bool?[]
                if (!bc.IsNullable)
                {
                    var result = new bool[bc.Length];
                    for (int i = 0; i < bc.Length; i++) result[i] = bc.Get(i);
                    return result;
                }
                else
                {
                    var result = new bool?[bc.Length];
                    for (int i = 0; i < bc.Length; i++)
                        result[i] = bc.IsNull(i) ? null : bc.Get(i);
                    return result;
                }
            }

            if (col is StringColumn sc)
            {
                // String is already a reference type, we can use ToArray
                // (StringColumn stores nulls directly in the array)
                return sc.Values.ToArray();
            }

            if (col is DateTimeColumn dtc)
            {
                if (!dtc.IsNullable) return dtc.Values.ToArray();

                var result = new DateTime?[dtc.Length];
                for (int i = 0; i < dtc.Length; i++)
                    result[i] = dtc.IsNull(i) ? null : dtc.Get(i);
                return result;
            }

            throw new NotSupportedException($"Writing column type '{col.DataType.Name}' to Parquet is not supported.");
        }
    }
}