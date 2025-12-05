namespace LeichtFrame.Core.Tests.Mocks;

public class SimpleMockColumn<T> : Column<T>
{
    private T[] _data;
    private bool[] _nulls;
    private int _count;

    public SimpleMockColumn(string name, int length, bool isNullable = true)
        : base(name, isNullable)
    {
        if (length < 0) throw new ArgumentOutOfRangeException(nameof(length));
        _data = new T[length];
        _nulls = new bool[length];
        _count = length;
    }

    public override int Length => _count;

    public override ReadOnlyMemory<T> Values => _data.AsMemory(0, _count);

    public override T Get(int index)
    {
        if ((uint)index >= (uint)_count) throw new IndexOutOfRangeException(nameof(index));
        return _data[index];
    }

    public override void SetValue(int index, T value)
    {
        if (index < 0) throw new IndexOutOfRangeException(nameof(index));
        if (index >= _data.Length) EnsureCapacity(index + 1);
        _data[index] = value;
        _nulls[index] = false;
        if (index >= _count) _count = index + 1;
    }

    public override bool IsNull(int index)
    {
        if ((uint)index >= (uint)_count) throw new IndexOutOfRangeException(nameof(index));
        return _nulls[index];
    }

    public override void SetNull(int index)
    {
        if (index < 0) throw new IndexOutOfRangeException(nameof(index));
        if (index >= _data.Length) EnsureCapacity(index + 1);
        _nulls[index] = true;
        if (index >= _count) _count = index + 1;
    }

    public override void SetNotNull(int index)
    {
        if ((uint)index >= (uint)_count) throw new IndexOutOfRangeException(nameof(index));
        _nulls[index] = false;
    }

    public override void EnsureCapacity(int capacity)
    {
        if (capacity <= _data.Length) return;
        int newSize = Math.Max(capacity, Math.Max(4, _data.Length * 2));
        Array.Resize(ref _data, newSize);
        Array.Resize(ref _nulls, newSize);
    }

    public override void Append(T value)
    {
        if (_count >= _data.Length)
            EnsureCapacity(_count + 1);

        _data[_count] = value;
        _nulls[_count] = false;
        _count++;
    }

    public override IColumn CloneSubset(IReadOnlyList<int> indices)
    {
        var newCol = new SimpleMockColumn<T>(Name, indices.Count, IsNullable);

        for (int i = 0; i < indices.Count; i++)
        {
            int sourceIndex = indices[i];

            if (IsNullable && IsNull(sourceIndex))
            {
                newCol.SetNull(i);
                newCol.SetValue(i, default!);
            }
            else
            {
                newCol.SetValue(i, Get(sourceIndex));
            }
        }

        return newCol;
    }
}
