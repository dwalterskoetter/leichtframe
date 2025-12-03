using LeichtFrame.Core;

namespace LeichtFrame.IO
{
    public static class DataFrameParquetExtensions
    {
        /// <summary>
        /// Writes the DataFrame to a Parquet file.
        /// </summary>
        public static void WriteParquet(this DataFrame df, string path)
        {
            ParquetWriter.Write(df, path);
        }

        /// <summary>
        /// Writes the DataFrame to a stream in Parquet format.
        /// </summary>
        public static void WriteParquet(this DataFrame df, Stream stream)
        {
            ParquetWriter.Write(df, stream);
        }

        /// <summary>
        /// Writes the DataFrame to a stream asynchronously.
        /// </summary>
        public static Task WriteParquetAsync(this DataFrame df, Stream stream)
        {
            return ParquetWriter.WriteAsync(df, stream);
        }
    }
}