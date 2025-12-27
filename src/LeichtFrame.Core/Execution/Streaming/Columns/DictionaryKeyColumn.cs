namespace LeichtFrame.Core.Execution.Streaming.Columns
{
    internal class DictionaryKeyColumn : IColumn<string?>, IFlyweightKeyColumn
    {
        private readonly string?[] _dictionary;
        private string? _currentValue;
        private bool _isNull;

        public string Name { get; }
        public Type DataType => typeof(string);
        public int Length => 1;
        public bool IsNullable => true;

        public DictionaryKeyColumn(string name, string?[] dictionary)
        {
            Name = name;
            _dictionary = dictionary;
        }

        public void SetData(int code, bool isNull)
        {
            if (isNull)
            {
                _isNull = true;
                _currentValue = null;
            }
            else
            {
                _isNull = false;
                _currentValue = _dictionary[code];
            }
        }

        public string? GetValue(int index) => _currentValue;
        object? IColumn.GetValue(int index) => _currentValue;

        public bool IsNull(int index) => _isNull;

        // Stubs for unsupported operations
        public void SetValue(int index, string? value) => throw new NotSupportedException();
        public ReadOnlySpan<string?> AsSpan() => throw new NotSupportedException();
        public ReadOnlyMemory<string?> Slice(int start, int length) => throw new NotSupportedException();
        public void Append(string? value) => throw new NotSupportedException();
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