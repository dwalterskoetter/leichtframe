using System;

namespace LeichtFrame.Core
{
    /// <summary>
    /// Provides helper extension methods for interacting with <see cref="IColumn"/> instances.
    /// </summary>
    public static class ColumnExtensions
    {
        /// <summary>
        /// Helper extension to get a typed value from a generic <see cref="IColumn"/>.
        /// Performs a cast and calls the typed GetValue method if possible.
        /// </summary>
        /// <typeparam name="T">The expected return type.</typeparam>
        /// <param name="column">The column instance.</param>
        /// <param name="index">The row index to retrieve.</param>
        /// <returns>The value of type T.</returns>
        /// <exception cref="InvalidCastException">Thrown if the column type does not match T.</exception>
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
        /// Handles type checking and dispatching to the concrete Append method.
        /// </summary>
        /// <param name="column">The target column.</param>
        /// <param name="value">The value to append (can be null if supported).</param>
        /// <exception cref="NotSupportedException">Thrown if the value type is incompatible or the column type is unknown.</exception>
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