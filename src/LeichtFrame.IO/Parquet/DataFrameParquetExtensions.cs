namespace LeichtFrame.IO
{
    public static class DataFrameParquetExtensions
    {
        // We do not actually need an extension on DataFrame for READ, 
        // since READ is static (factory).
        // But for consistency, we could prepare WriteParquet here.

        // FFor ReadParquet, the call via ParquetReader.Read(...) is the clean way.
        // If you want df.WriteParquet, that comes in C.2.2.

        // We leave the file empty or as a placeholder for the writer later.
    }
}