namespace LeichtFrame.Core
{
    public static class ColumnExtensions
    {
        /// <summary>
        /// Helper extension to get a typed value from a generic IColumn.
        /// Performs a cast and calls the typed GetValue method.
        /// </summary>
        public static T Get<T>(this IColumn column, int index)
        {
            // Fast Path: If it is already the correct typed interface
            if (column is IColumn<T> typedCol)
            {
                return typedCol.GetValue(index);
            }

            // Slow Path: Type does not match or is unknown -> Exception or Convert
            throw new InvalidCastException(
                $"Column '{column.Name}' is of type {column.DataType.Name}, but '{typeof(T).Name}' was requested.");
        }
    }
}