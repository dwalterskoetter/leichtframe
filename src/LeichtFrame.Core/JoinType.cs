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
        Inner,

        /// <summary>
        /// Returns all records from the left table, and the matched records from the right table. 
        /// Unmatched records from the right side are null.
        /// </summary>
        Left
    }
}