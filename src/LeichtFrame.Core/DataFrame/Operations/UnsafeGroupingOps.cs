namespace LeichtFrame.Core
{
    /// <summary>
    /// Experimental High-Performance Operations (God Mode).
    /// These methods use unsafe code and advanced algorithms for maximum performance.
    /// </summary>
    public static class UnsafeGroupingOps
    {
        /// <summary>
        /// Forces the use of the Lock-Free Radix Partitioning Algorithm.
        /// (Corresponds to the "God Mode" Strategy).
        /// </summary>
        public static GroupedDataFrame GroupByRadix(this DataFrame df, string columnName)
        {
            return new RadixSortStrategy().Group(df, columnName);
        }
    }
}