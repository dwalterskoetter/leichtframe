namespace LeichtFrame.Core.Execution.Streaming.Columns
{
    internal class IndirectScalarColumn<T> : IColumn<T>, IFlyweightKeyColumn
    {
        private readonly IColumn<T> _source;
        private int _currentRowIndex;
        private bool _isNull;

        public string Name { get; }
        public Type DataType => typeof(T);
        public int Length => 1;
        public bool IsNullable => true;

        public IndirectScalarColumn(string name, IColumn<T> source)
        {
            Name = name;
            _source = source;
        }

        public void SetData(int rowIndex, bool isNull)
        {
            _currentRowIndex = rowIndex;
            _isNull = isNull;
        }

        public T GetValue(int index) => _source.GetValue(_currentRowIndex);
        object? IColumn.GetValue(int index) => _isNull ? null : _source.GetValue(_currentRowIndex);
        public bool IsNull(int index) => _isNull;

        // Stubs
        public void SetValue(int index, T value) => throw new NotSupportedException();
        public ReadOnlySpan<T> AsSpan() => throw new NotSupportedException();
        public ReadOnlyMemory<T> Slice(int start, int length) => throw new NotSupportedException();
        public void Append(T value) => throw new NotSupportedException();
        public void AppendObject(object? value) => throw new NotSupportedException();
        public void SetNull(int index) => throw new NotSupportedException();
        public void EnsureCapacity(int capacity) { }
        public IColumn CloneSubset(IReadOnlyList<int> indices) => throw new NotSupportedException();
        public object? ComputeSum(int[] indices, int start, int end) => null;
        public object? ComputeMean(int[] indices, int start, int end) => null;
        public object? ComputeMin(int[] indices, int start, int end) => null;
        public object? ComputeMax(int[] indices, int start, int end) => null;
    }
}