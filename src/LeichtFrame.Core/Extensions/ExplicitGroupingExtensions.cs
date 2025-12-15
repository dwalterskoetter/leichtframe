namespace LeichtFrame.Core
{
    /// <summary>
    /// Extensions for explicit grouping (Old Way) to compare with new methods.
    /// </summary>
    public static class ExplicitGroupingExtensions
    {
        /// <summary>
        /// Alias for the Standard Implementation (calls your unchanged GroupBy).
        /// needed for Benchmarks to explicitly compare "Old Way".
        /// </summary>
        public static GroupedDataFrame GroupByStandard(this DataFrame df, string columnName)
        {
            return df.GroupBy(columnName);
        }
    }
}