using System;
using LeichtFrame.Core;

namespace LeichtFrame.Core.Tests.Mocks;

// A simple, inefficient implementation just to test the Base Class API contract.
public class SimpleMockColumn<T> : Column<T>
{
    private readonly T[] _data;
    private readonly bool[] _nulls;

    public SimpleMockColumn(string name, int length) : base(name)
    {
        _data = new T[length];
        _nulls = new bool[length];
    }

    public override int Length => _data.Length;

    public override ReadOnlyMemory<T> Values => _data.AsMemory();

    public override T GetValue(int index) => _data[index];

    public override void SetValue(int index, T value)
    {
        _data[index] = value;
        _nulls[index] = false;
    }

    public override bool IsNull(int index) => _nulls[index];

    public override void SetNull(int index) => _nulls[index] = true;

    public override void SetNotNull(int index) => _nulls[index] = false;
}