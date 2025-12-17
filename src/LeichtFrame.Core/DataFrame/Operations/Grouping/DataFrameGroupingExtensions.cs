using LeichtFrame.Core.Logic;

namespace LeichtFrame.Core
{
    /// <summary>
    /// Benchmarking-only: Exposes the SIMD-optimized Radix Sort Grouping from the Engine layer.
    /// </summary>
    public static class DataFrameGroupingExtensions
    {
        /// <summary>
        /// Benchmarking only: Executes the SIMD-optimized Radix Sort Strategy.
        /// </summary>
        public static GroupedDataFrame GroupBy(this DataFrame df, string columnName)
        {
            if (string.IsNullOrEmpty(columnName)) throw new ArgumentNullException(nameof(columnName));

            return GroupByDispatcher.DecideAndExecute(df, columnName);
        }
    }
}