using LeichtFrame.Core;

namespace LeichtFrame.IO
{
    /// <summary>
    /// Provides extension methods for exporting <see cref="DataFrame"/> objects to Apache Parquet format.
    /// </summary>
    public static class DataFrameParquetExtensions
    {
        /// <summary>
        /// Writes the DataFrame to a Parquet file at the specified path.
        /// </summary>
        /// <param name="df">The source DataFrame to export.</param>
        /// <param name="path">The file path where the Parquet file will be created or overwritten.</param>
        public static void WriteParquet(this DataFrame df, string path)
        {
            ParquetWriter.Write(df, path);
        }

        /// <summary>
        /// Writes the DataFrame to a stream in Parquet format.
        /// </summary>
        /// <param name="df">The source DataFrame to export.</param>
        /// <param name="stream">The writable output stream.</param>
        public static void WriteParquet(this DataFrame df, Stream stream)
        {
            ParquetWriter.Write(df, stream);
        }

        /// <summary>
        /// Writes the DataFrame to a stream asynchronously in Parquet format.
        /// Recommended for Web APIs to avoid blocking threads during I/O.
        /// </summary>
        /// <param name="df">The source DataFrame to export.</param>
        /// <param name="stream">The writable output stream.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        public static Task WriteParquetAsync(this DataFrame df, Stream stream)
        {
            return ParquetWriter.WriteAsync(df, stream);
        }
    }
}