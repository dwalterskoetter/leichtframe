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

        /// <summary>
        /// Appends an untyped object value to the column. 
        /// Throws if the value type does not match the column type.
        /// </summary>
        public static void AppendObject(this IColumn column, object? value)
        {
            if (value == null)
            {
                // We must unfortunately know which concrete types support Append(null).
                // Since IColumn does not have Append (only the concrete classes), we cast.
                if (column is IntColumn ic) ic.Append(null);
                else if (column is DoubleColumn dc) dc.Append(null);
                else if (column is StringColumn sc) sc.Append(null);
                else if (column is BoolColumn bc) bc.Append(null);
                else if (column is DateTimeColumn dtc) dtc.Append(null);
                else throw new NotSupportedException($"Column '{column.Name}' does not support null values or type is unknown.");
                return;
            }

            // Type dispatch
            if (column is IntColumn i) i.Append((int)value);
            else if (column is DoubleColumn d) d.Append(Convert.ToDouble(value)); // Convert allows int->double
            else if (column is StringColumn s) s.Append(value.ToString());
            else if (column is BoolColumn b) b.Append((bool)value);
            else if (column is DateTimeColumn dt) dt.Append((DateTime)value);
            else
                throw new NotSupportedException($"Cannot append object of type {value.GetType().Name} to column {column.GetType().Name}");
        }
    }
}