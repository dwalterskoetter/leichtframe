using Apache.Arrow;
using LeichtFrame.Core;

namespace LeichtFrame.IO
{
    public static class ArrowExtensions
    {
        /// <summary>
        /// Extension method to convert an Arrow RecordBatch directly to a LeichtFrame DataFrame.
        /// </summary>
        public static DataFrame ToDataFrame(this RecordBatch batch)
        {
            return ArrowConverter.ToDataFrame(batch);
        }

        /// <summary>
        /// Converts the DataFrame to an Arrow RecordBatch.
        /// </summary>
        public static RecordBatch ToArrow(this DataFrame df)
        {
            return ArrowConverter.ToRecordBatch(df);
        }
    }
}