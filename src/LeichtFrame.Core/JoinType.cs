namespace LeichtFrame.Core
{
    /// <summary>
    /// Specifies the type of join operation to perform when combining two DataFrames.
    /// </summary>
    public enum JoinType
    {
        /// <summary>
        /// Returns records that have matching values in both tables.
        /// </summary>
        Inner
        // Left, Right, Full (tbd)
    }
}