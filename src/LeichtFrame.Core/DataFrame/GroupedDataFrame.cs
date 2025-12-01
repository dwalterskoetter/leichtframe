namespace LeichtFrame.Core
{
    /// <summary>
    /// Represents an intermediate state of a DataFrame grouped by a specific column.
    /// Used to perform aggregated calculations per group.
    /// </summary>
    public class GroupedDataFrame
    {
        private readonly Dictionary<object, List<int>> _groupMap;

        /// <summary>
        /// The original DataFrame.
        /// </summary>
        public DataFrame Source { get; }

        /// <summary>
        /// The name of the column used for grouping.
        /// </summary>
        public string GroupColumnName { get; }

        /// <summary>
        /// Access to the internal grouping map.
        /// Key: The value of the group (e.g., "Berlin" or 42).
        /// Value: List of row indices belonging to this group.
        /// </summary>
        public IReadOnlyDictionary<object, List<int>> GroupMap => _groupMap;

        internal GroupedDataFrame(DataFrame source, string groupColName, Dictionary<object, List<int>> groupMap)
        {
            Source = source;
            GroupColumnName = groupColName;
            _groupMap = groupMap;
        }
    }
}