using System.Globalization;

namespace LeichtFrame.Core;

/// <summary>
/// Typed base class for columns storing specific data types (int, double, string, etc.).
/// </summary>
public abstract class Column<T> : Column, IColumn<T>
{
    protected Column(string name, bool isNullable = false) : base(name, typeof(T), isNullable)
    {
    }

    public abstract ReadOnlyMemory<T> Values { get; }

    public abstract T Get(int index);
    public abstract void SetValue(int index, T value);

    // --- Interface Implementations ---

    T IColumn<T>.GetValue(int index) => Get(index);

    public override object? GetValue(int index)
    {
        if (IsNullable && IsNull(index)) return null;
        return Get(index);
    }

    // --- Appending ---

    public abstract void Append(T value);

    // WICHTIG: Hier muss 'override' stehen, da es in 'Column' abstract ist.
    public override void AppendObject(object? value)
    {
        if (value is T typedVal)
        {
            Append(typedVal);
        }
        else if (value is null)
        {
            if (!IsNullable)
                throw new ArgumentException($"Cannot append null to non-nullable column '{Name}'.");

            Append(default!);
            SetNull(Length - 1);
        }
        else
        {
            try
            {
                var converted = (T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture);
                Append(converted);
            }
            catch
            {
                throw new ArgumentException($"Cannot convert '{value}' to {typeof(T).Name}");
            }
        }
    }

    // --- Null Handling ---

    public abstract override bool IsNull(int index);
    public abstract override void SetNull(int index);
    public abstract void SetNotNull(int index);

    // --- Slicing ---

    public virtual ReadOnlyMemory<T> Slice(int start, int length)
    {
        if ((uint)start > (uint)Length || (uint)length > (uint)(Length - start))
        {
            throw new ArgumentOutOfRangeException(nameof(start),
                $"Slice range {start}..{start + length} is out of bounds (Length: {Length}).");
        }

        return Values.Slice(start, length);
    }

    public virtual ReadOnlySpan<T> AsSpan() => Values.Span;
}