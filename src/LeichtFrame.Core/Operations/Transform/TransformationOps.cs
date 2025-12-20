using System.Runtime.CompilerServices;

namespace LeichtFrame.Core
{
    /// <summary>
    /// Provides extension methods for transforming data and adding computed columns.
    /// </summary>
    public static class TransformationOps
    {
        /// <summary>
        /// Creates a new column by applying a function to every row and adds it to the DataFrame.
        /// Returns a new DataFrame instance (the original remains unchanged).
        /// </summary>
        /// <typeparam name="T">The type of the new column (e.g., int, double, string).</typeparam>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="newColumnName">The name of the new column.</param>
        /// <param name="computer">A function that takes a RowView and returns the calculated value.</param>
        /// <returns>A new DataFrame containing the original columns plus the new computed column.</returns>
        public static DataFrame AddColumn<T>(this DataFrame df, string newColumnName, Func<RowView, T> computer)
        {
            if (string.IsNullOrEmpty(newColumnName)) throw new ArgumentNullException(nameof(newColumnName));
            if (computer == null) throw new ArgumentNullException(nameof(computer));

            if (df.HasColumn(newColumnName))
            {
                throw new ArgumentException($"Column '{newColumnName}' already exists in DataFrame.");
            }

            Type typeT = typeof(T);
            Type underlying = Nullable.GetUnderlyingType(typeT) ?? typeT;
            bool isNullable = underlying != typeT || !typeT.IsValueType; // e.g. int? or string

            // 1. Create the Column (Non-Generic Factory)
            // Wir nutzen hier Create(Type), das dank deines Fixes Nullable Types auspacken kann.
            IColumn newCol = ColumnFactory.Create(newColumnName, typeT, df.RowCount, isNullable);

            // 2. Compute & Append loop (Type-Dispatcher)

            // Case A: Exact Match (e.g. T=int, Col=IColumn<int>)
            // Auch StringColumn fällt hierunter (T=string, Col=IColumn<string>)
            if (newCol is IColumn<T> typedCol)
            {
                for (int i = 0; i < df.RowCount; i++)
                {
                    var row = new RowView(i, df.Columns, df.Schema);
                    typedCol.Append(computer(row));
                }
            }
            // Case B: Nullable Primitives (e.g. T=int?, Col=IntColumn)
            // IntColumn implementiert NICHT IColumn<int?>, hat aber eine Methode Append(int?).
            // Wir nutzen Unsafe.As, um den Delegaten zu casten ohne Boxing.
            else if (newCol is IntColumn ic && typeT == typeof(int?))
            {
                var func = Unsafe.As<Func<RowView, T>, Func<RowView, int?>>(ref computer);
                for (int i = 0; i < df.RowCount; i++)
                    ic.Append(func(new RowView(i, df.Columns, df.Schema)));
            }
            else if (newCol is DoubleColumn dc && typeT == typeof(double?))
            {
                var func = Unsafe.As<Func<RowView, T>, Func<RowView, double?>>(ref computer);
                for (int i = 0; i < df.RowCount; i++)
                    dc.Append(func(new RowView(i, df.Columns, df.Schema)));
            }
            else if (newCol is BoolColumn bc && typeT == typeof(bool?))
            {
                var func = Unsafe.As<Func<RowView, T>, Func<RowView, bool?>>(ref computer);
                for (int i = 0; i < df.RowCount; i++)
                    bc.Append(func(new RowView(i, df.Columns, df.Schema)));
            }
            else if (newCol is DateTimeColumn dtc && typeT == typeof(DateTime?))
            {
                var func = Unsafe.As<Func<RowView, T>, Func<RowView, DateTime?>>(ref computer);
                for (int i = 0; i < df.RowCount; i++)
                    dtc.Append(func(new RowView(i, df.Columns, df.Schema)));
            }
            else
            {
                // Fallback (Safe but slower due to boxing)
                for (int i = 0; i < df.RowCount; i++)
                {
                    var row = new RowView(i, df.Columns, df.Schema);
                    object? val = computer(row);
                    newCol.AppendObject(val);
                }
            }

            // 3. Construct new DataFrame
            var newColumnList = new List<IColumn>(df.Columns);
            newColumnList.Add(newCol);

            return new DataFrame(newColumnList);
        }
    }
}