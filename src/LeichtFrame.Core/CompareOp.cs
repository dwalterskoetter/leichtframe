namespace LeichtFrame.Core
{
    /// <summary>
    /// Defines the comparison operations available for vectorized filtering.
    /// </summary>
    public enum CompareOp
    {
        /// <summary>
        /// Checks if the value is equal to the target.
        /// </summary>
        Equal,

        /// <summary>
        /// Checks if the value is not equal to the target.
        /// </summary>
        NotEqual,

        /// <summary>
        /// Checks if the value is strictly greater than the target.
        /// </summary>
        GreaterThan,

        /// <summary>
        /// Checks if the value is greater than or equal to the target.
        /// </summary>
        GreaterThanOrEqual,

        /// <summary>
        /// Checks if the value is strictly less than the target.
        /// </summary>
        LessThan,

        /// <summary>
        /// Checks if the value is less than or equal to the target.
        /// </summary>
        LessThanOrEqual
    }
}