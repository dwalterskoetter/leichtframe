namespace LeichtFrame.Core.Execution.Streaming.Columns
{
    internal class ScalarResultColumn : IColumn, IColumn<int>, IColumn<double>
    {
        private int _valInt;
        private double _valDouble;
        private readonly bool _isInt;

        public string Name { get; }
        public Type DataType { get; }
        public int Length => 1;
        public bool IsNullable => false;

        public ScalarResultColumn(string name, Type type)
        {
            Name = name;
            DataType = type;
            _isInt = type == typeof(int);
        }

        public void SetInt(int v) => _valInt = v;
        public void SetDouble(double v) => _valDouble = v;

        public object? GetValue(int index) => _isInt ? _valInt : _valDouble;
        int IColumn<int>.GetValue(int index) => _valInt;
        double IColumn<double>.GetValue(int index) => _valDouble;

        public bool IsNull(int index) => false;

        // Stubs
        public void AppendObject(object? value) => throw new NotSupportedException();
        public void EnsureCapacity(int capacity) { }
        public IColumn CloneSubset(IReadOnlyList<int> indices) => throw new NotSupportedException();
        public void SetNull(int index) => throw new NotSupportedException();
        public void SetValue(int index, int value) => throw new NotSupportedException();
        public void Append(int value) => throw new NotSupportedException();
        ReadOnlyMemory<int> IColumn<int>.Slice(int start, int length) => throw new NotSupportedException();
        ReadOnlySpan<int> IColumn<int>.AsSpan() => throw new NotSupportedException();
        public void SetValue(int index, double value) => throw new NotSupportedException();
        public void Append(double value) => throw new NotSupportedException();
        ReadOnlyMemory<double> IColumn<double>.Slice(int start, int length) => throw new NotSupportedException();
        ReadOnlySpan<double> IColumn<double>.AsSpan() => throw new NotSupportedException();
        public object? ComputeSum(int[] indices, int start, int end) => null;
        public object? ComputeMean(int[] indices, int start, int end) => null;
        public object? ComputeMin(int[] indices, int start, int end) => null;
        public object? ComputeMax(int[] indices, int start, int end) => null;
    }
}