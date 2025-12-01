namespace LeichtFrame.Core.Tests.Mocks;

public class SimpleMockColumn<T> : Column<T>
{
    private readonly T[] _data;
    private readonly bool[] _nulls;

    public SimpleMockColumn(string name, int length, bool isNullable = true)
        : base(name, isNullable)
    {
        _data = new T[length];
        _nulls = new bool[length];
    }

    public override int Length => _data.Length;
    public override ReadOnlyMemory<T> Values => _data.AsMemory();
    public override T Get(int index) => _data[index];
    public override void SetValue(int index, T value)
    {
        _data[index] = value;
        _nulls[index] = false;
    }
    public override bool IsNull(int index) => _nulls[index];
    public override void SetNull(int index) => _nulls[index] = true;
    public override void SetNotNull(int index) => _nulls[index] = false;
    public override void EnsureCapacity(int capacity) { }
    public override IColumn CloneSubset(IReadOnlyList<int> indices)
    {
        // New mock column created with the appropriate size
        var newCol = new SimpleMockColumn<T>(Name, indices.Count, IsNullable);

        for (int i = 0; i < indices.Count; i++)
        {
            int sourceIndex = indices[i];

            if (IsNullable && IsNull(sourceIndex))
            {
                newCol.SetNull(i);
                // In mock, value usually doesn't matter when null, but we set default
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