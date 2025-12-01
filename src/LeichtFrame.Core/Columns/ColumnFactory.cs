using System;
using System.Reflection.Metadata;

namespace LeichtFrame.Core
{
    public static class ColumnFactory
    {
        /// Creates a concrete column instance based on the provided type.
        public static IColumn Create(string name, Type type, int capacity = 16, bool isNullable = false)
        {
            if (type == typeof(int))
                return new IntColumn(name, capacity, isNullable);

            if (type == typeof(double))
                return new DoubleColumn(name, capacity, isNullable);

            if (type == typeof(bool))
                return new BoolColumn(name, capacity, isNullable);

            if (type == typeof(string))
                return new StringColumn(name, capacity, isNullable);

            if (type == typeof(DateTime))
                return new DateTimeColumn(name, capacity, isNullable);

            throw new NotSupportedException($"Type {type.Name} is not supported yet.");
        }

        /// Generic overload for convenience.
        public static IColumn<T> Create<T>(string name, int capacity = 16, bool isNullable = false)
        {
            return (IColumn<T>)Create(name, typeof(T), capacity, isNullable);
        }
    }
}