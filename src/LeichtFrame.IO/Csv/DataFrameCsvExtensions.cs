using LeichtFrame.Core;

namespace LeichtFrame.IO
{
    public static class DataFrameCsvExtensions
    {
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
    }
}