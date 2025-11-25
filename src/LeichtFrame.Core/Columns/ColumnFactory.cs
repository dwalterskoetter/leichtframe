using System;

namespace LeichtFrame.Core
{
    public static class ColumnFactory
    {
        /// Creates a concrete column instance based on the provided type.
        public static IColumn Create(string name, Type type, int capacity = 16)
        {
            if (type == typeof(int))
                return new IntColumn(name, capacity);

            if (type == typeof(double))
                return new DoubleColumn(name, capacity);

            if (type == typeof(bool))
                return new BoolColumn(name, capacity);

            if (type == typeof(string))
                return new StringColumn(name, capacity);

            throw new NotSupportedException($"Type {type.Name} is not supported yet.");
        }

        /// Generic overload for convenience.
        public static IColumn<T> Create<T>(string name, int capacity = 16)
        {
            return (IColumn<T>)Create(name, typeof(T), capacity);
        }
    }
}