using LeichtFrame.Core.Engine.Kernels.GroupBy;

namespace LeichtFrame.Core.Operations.GroupBy
{
    /// <summary>
    /// Exposes single- and multi-column group by operations
    /// </summary>
    public static class GroupingOps
    {
        /// <summary>
        /// Single-column GroupBy
        /// </summary>
        /// <param name="df"></param>
        /// <param name="columnName"></param>
        /// /// <exception cref="ArgumentNullException"></exception>
        public static GroupedDataFrame GroupBy(this DataFrame df, string columnName)
        {
            if (string.IsNullOrEmpty(columnName)) throw new ArgumentNullException(nameof(columnName));

            return GroupByDispatcher.DecideAndExecute(df, columnName);
        }

        /// <summary>
        /// Multi-column GroupBy
        /// </summary>
        /// <param name="df"></param>
        /// <param name="columnNames"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        public static GroupedDataFrame GroupBy(this DataFrame df, params string[] columnNames)
        {
            if (columnNames == null || columnNames.Length == 0)
                throw new ArgumentException("At least one column required.");

            return GroupByDispatcher.DecideAndExecute(df, columnNames);
        }
    }
}