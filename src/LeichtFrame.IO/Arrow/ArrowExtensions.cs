using Apache.Arrow;
using LeichtFrame.Core;

namespace LeichtFrame.IO
{
    /// <summary>
    /// Provides extension methods for seamless integration with Apache Arrow.
    /// Allows converting <see cref="RecordBatch"/> to <see cref="DataFrame"/> and vice versa via fluent syntax.
    /// </summary>
    public static class ArrowExtensions
    {
        /// <summary>
        /// Converts an Apache Arrow <see cref="RecordBatch"/> directly into a LeichtFrame <see cref="DataFrame"/>.
        /// </summary>
        /// <param name="batch">The source Arrow RecordBatch.</param>
        /// <returns>A new DataFrame containing the data from the batch.</returns>
        /// <exception cref="ArgumentNullException">Thrown if the batch is null.</exception>
        public static DataFrame ToDataFrame(this RecordBatch batch)
        {
            return ArrowConverter.ToDataFrame(batch);
        }

        /// <summary>
        /// Converts the <see cref="DataFrame"/> into an Apache Arrow <see cref="RecordBatch"/>.
        /// Useful for passing data to other libraries like ML.NET, Spark, or Python (via interop).
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <returns>A new Arrow RecordBatch representing the DataFrame.</returns>
        /// <exception cref="ArgumentNullException">Thrown if the DataFrame is null.</exception>
        public static RecordBatch ToArrow(this DataFrame df)
        {
            return ArrowConverter.ToRecordBatch(df);
        }
    }
}