namespace LeichtFrame.Core
{
    /// <summary>
    /// Factory class to create concrete column instances based on runtime types.
    /// Acts as the central registry for supported column types.
    /// </summary>
    public static class ColumnFactory
    {
        /// <summary>
        /// Creates a concrete column instance (e.g. <see cref="IntColumn"/>) based on the provided CLR type.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <param name="type">The data type (e.g. typeof(int)). Supported: int, double, bool, string, DateTime.</param>
        /// <param name="capacity">The initial capacity (number of rows) to allocate.</param>
        /// <param name="isNullable">Whether the column should support null values.</param>
        /// <returns>An <see cref="IColumn"/> instance containing the specific implementation.</returns>
        /// <exception cref="NotSupportedException">Thrown if the provided type is not supported by LeichtFrame.</exception>
        public static IColumn Create(string name, Type type, int capacity = 16, bool isNullable = false)
        {
            // WICHTIG: Nullable Typen auspacken (z.B. int? -> int)
            Type underlyingType = Nullable.GetUnderlyingType(type) ?? type;

            if (underlyingType == typeof(int))
                return new IntColumn(name, capacity, isNullable);

            if (underlyingType == typeof(double))
                return new DoubleColumn(name, capacity, isNullable);

            if (underlyingType == typeof(bool))
                return new BoolColumn(name, capacity, isNullable);

            if (underlyingType == typeof(string))
                return new StringColumn(name, capacity, isNullable);

            if (underlyingType == typeof(DateTime))
                return new DateTimeColumn(name, capacity, isNullable);

            throw new NotSupportedException($"Type {type.Name} is not supported yet.");
        }

        /// <summary>
        /// Generic convenience overload to create a strongly-typed column.
        /// </summary>
        /// <typeparam name="T">The data type of the column.</typeparam>
        /// <param name="name">The name of the column.</param>
        /// <param name="capacity">The initial capacity to allocate.</param>
        /// <param name="isNullable">Whether the column should support null values.</param>
        /// <returns>A typed <see cref="IColumn{T}"/> instance.</returns>
        public static IColumn<T> Create<T>(string name, int capacity = 16, bool isNullable = false)
        {
            return (IColumn<T>)Create(name, typeof(T), capacity, isNullable);
        }
    }
}