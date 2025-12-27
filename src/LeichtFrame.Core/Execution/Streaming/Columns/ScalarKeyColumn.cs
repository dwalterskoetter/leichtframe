namespace LeichtFrame.Core.Execution.Streaming.Columns
{
    internal class ScalarKeyColumn<T> : IColumn<T>, IFlyweightKeyColumn
    {
        public T Value;
        public bool IsNullValue;

        public string Name { get; }
        public Type DataType => typeof(T);
        public int Length => 1;
        public bool IsNullable => true;

        public ScalarKeyColumn(string name)
        {
            Name = name;
            Value = default!;
        }

        public void SetData(int key, bool isNull)
        {
            // Cast ugly but works for Int fast path without boxing in Generic context
            if (typeof(T) == typeof(int)) Value = (T)(object)key;
            IsNullValue = isNull;
        }

        public T GetValue(int index) => Value;
        object? IColumn.GetValue(int index) => IsNullValue ? null : Value;
        public void SetValue(int index, T value) => Value = value;
        public bool IsNull(int index) => IsNullValue;

        // Stubs for unsupported operations
        public void Append(T value) => throw new NotSupportedException();
        public void AppendObject(object? value) => throw new NotSupportedException();
        public void SetNull(int index) => throw new NotSupportedException();
        public void EnsureCapacity(int capacity) { }
        public IColumn CloneSubset(IReadOnlyList<int> indices) => throw new NotSupportedException();
        public ReadOnlySpan<T> AsSpan() => throw new NotSupportedException();
        public ReadOnlyMemory<T> Slice(int start, int length) => throw new NotSupportedException();
        public object? ComputeSum(int[] indices, int start, int end) => null;
        public object? ComputeMean(int[] indices, int start, int end) => null;
        public object? ComputeMin(int[] indices, int start, int end) => null;
        public object? ComputeMax(int[] indices, int start, int end) => null;
    }
}