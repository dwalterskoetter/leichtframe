using System;

namespace LeichtFrame.Core
{
    // STUBS for concrete column types.
    // These do not implement any storage or functionality.

    public class DoubleColumn : Column<double>
    {
        public DoubleColumn(string name, int capacity = 0) : base(name) { }
        public override int Length => 0;
        public override ReadOnlyMemory<double> Values => ReadOnlyMemory<double>.Empty;
        public override double GetValue(int index) => 0;
        public override void SetValue(int index, double value) { }
        public override void EnsureCapacity(int capacity) { }
        public override bool IsNull(int index) => false;
        public override void SetNull(int index) { }
        public override void SetNotNull(int index) { }
    }

    public class StringColumn : Column<string>
    {
        public StringColumn(string name, int capacity = 0) : base(name) { }
        public override int Length => 0;
        public override ReadOnlyMemory<string> Values => ReadOnlyMemory<string>.Empty;
        public override string GetValue(int index) => null;
        public override void SetValue(int index, string value) { }
        public override void EnsureCapacity(int capacity) { }
        public override bool IsNull(int index) => false;
        public override void SetNull(int index) { }
        public override void SetNotNull(int index) { }
    }

    public class BoolColumn : Column<bool>
    {
        public BoolColumn(string name, int capacity = 0) : base(name) { }
        public override int Length => 0;
        public override ReadOnlyMemory<bool> Values => ReadOnlyMemory<bool>.Empty;
        public override bool GetValue(int index) => false;
        public override void SetValue(int index, bool value) { }
        public override void EnsureCapacity(int capacity) { }
        public override bool IsNull(int index) => false;
        public override void SetNull(int index) { }
        public override void SetNotNull(int index) { }
    }
}