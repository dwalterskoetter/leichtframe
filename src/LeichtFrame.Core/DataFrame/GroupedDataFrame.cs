namespace LeichtFrame.Core
{
    /// <summary>
    /// Represents an intermediate state of a DataFrame grouped by a specific column.
    /// Uses CSR (Compressed Sparse Row) format for high-performance aggregations.
    /// </summary>
    public abstract class GroupedDataFrame
    {
        /// <summary>
        /// Gets the source DataFrame that was grouped.
        /// </summary>
        public DataFrame Source { get; }

        /// <summary>
        /// Gets the name of the column used for grouping.
        /// </summary>
        public string GroupColumnName { get; }

        /// <summary>
        /// Contains row indices belonging to the 'null' group, if any exist.
        /// This is separate from the main CSR structure to avoid boxing keys.
        /// </summary>
        public int[]? NullGroupIndices { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="GroupedDataFrame"/> class.
        /// </summary>
        /// <param name="source">The source DataFrame.</param>
        /// <param name="groupColName">The name of the grouping column.</param>
        /// <param name="nullIndices">Optional array of row indices belonging to the null group.</param>
        protected GroupedDataFrame(DataFrame source, string groupColName, int[]? nullIndices)
        {
            Source = source;
            GroupColumnName = groupColName;
            NullGroupIndices = nullIndices;
        }

        // --- Public Accessors for Aggregation Engines ---

        /// <summary>
        /// Gets the number of unique groups found (excluding the null group).
        /// </summary>
        public abstract int GroupCount { get; }

        /// <summary>
        /// Gets the raw array of group keys.
        /// </summary>
        public abstract Array GetKeys();

        /// <summary>
        /// CSR Offsets: Points to the start index of each group in <see cref="RowIndices"/>.
        /// Length is GroupCount + 1.
        /// </summary>
        public abstract int[] GroupOffsets { get; }

        /// <summary>
        /// CSR Indices: A flattened array containing row indices for all groups.
        /// </summary>
        public abstract int[] RowIndices { get; }
    }

    /// <summary>
    /// High-performance typed grouping result (CSR format).
    /// </summary>
    internal class GroupedDataFrame<TKey> : GroupedDataFrame
    {
        private readonly TKey[] _keys;
        private readonly int[] _offsets;
        private readonly int[] _indices;

        public GroupedDataFrame(DataFrame source, string groupColName, TKey[] keys, int[] offsets, int[] indices, int[]? nullIndices)
            : base(source, groupColName, nullIndices)
        {
            _keys = keys;
            _offsets = offsets;
            _indices = indices;
        }

        public override int GroupCount => _keys.Length;
        public override Array GetKeys() => _keys;
        public override int[] GroupOffsets => _offsets;
        public override int[] RowIndices => _indices;
    }
}