namespace LeichtFrame.Core
{
    /// <summary>
    /// A zero-copy view over specific rows of another column.
    /// Uses an index map (indirection array) to point to the original data.
    /// <para>
    /// ⚠️ Limitations: 
    /// 1. Access is slightly slower due to double lookup.
    /// 2. Does NOT support contiguous Span/Memory access (.Values throws).
    /// </para>
    /// </summary>
    /// <typeparam name="T">The type of data stored in the column.</typeparam>
    public class IndirectColumn<T> : IColumn<T>, IDisposable
    {
        private readonly IColumn<T> _source;
        private readonly int[] _indices;

        /// <summary>
        /// Initializes a new instance of the <see cref="IndirectColumn{T}"/> class.
        /// </summary>
        /// <param name="source">The underlying source column.</param>
        /// <param name="indices">The indices map representing the view.</param>
        public IndirectColumn(IColumn<T> source, int[] indices)
        {
            _source = source ?? throw new ArgumentNullException(nameof(source));
            _indices = indices ?? throw new ArgumentNullException(nameof(indices));
        }

        /// <inheritdoc />
        public string Name => _source.Name;

        /// <inheritdoc />
        public Type DataType => _source.DataType;

        /// <inheritdoc />
        public int Length => _indices.Length;

        /// <inheritdoc />
        public bool IsNullable => _source.IsNullable;

        /// <summary>
        /// Not supported for IndirectColumn as data is scattered.
        /// </summary>
        public ReadOnlyMemory<T> Values => throw new NotSupportedException(
            "IndirectColumn does not support contiguous memory access. Materialize this column first.");

        /// <inheritdoc />
        public ReadOnlySpan<T> AsSpan() => throw new NotSupportedException(
            "IndirectColumn does not support Span access.");

        // --- Data Access ---

        /// <summary>
        /// Gets the strongly-typed value at the specified view index.
        /// </summary>
        public T Get(int index)
        {
            int realIndex = _indices[index];
            return _source.GetValue(realIndex);
        }

        // Explicit Interface Implementation to satisfy IColumn<T>
        T IColumn<T>.GetValue(int index) => Get(index);

        /// <inheritdoc />
        public object? GetValue(int index)
        {
            int realIndex = _indices[index];
            return _source.GetValue(realIndex);
        }

        /// <inheritdoc />
        public void SetValue(int index, T value)
        {
            int realIndex = _indices[index];
            _source.SetValue(realIndex, value);
        }

        // --- Null Handling ---

        /// <inheritdoc />
        public bool IsNull(int index)
        {
            int realIndex = _indices[index];
            return _source.IsNull(realIndex);
        }

        /// <inheritdoc />
        public void SetNull(int index)
        {
            int realIndex = _indices[index];
            _source.SetNull(realIndex);
        }

        // --- Mutation (Not Supported) ---

        /// <summary>
        /// Not supported for Indirect View.
        /// </summary>
        public void Append(T value) => throw new NotSupportedException("Cannot append to an Indirect View.");

        /// <summary>
        /// Not supported for Indirect View.
        /// </summary>
        public void AppendObject(object? value) => throw new NotSupportedException("Cannot append to an Indirect View.");

        /// <summary>
        /// Not supported for Indirect View.
        /// </summary>
        public void EnsureCapacity(int capacity) => throw new NotSupportedException("Cannot resize an Indirect View.");

        // --- Slicing & Cloning ---

        /// <inheritdoc />
        public ReadOnlyMemory<T> Slice(int start, int length)
        {
            throw new NotSupportedException("Cannot slice an IndirectColumn safely to Memory.");
        }

        /// <inheritdoc />
        public IColumn CloneSubset(IReadOnlyList<int> indices)
        {
            // Deep Clone: Materialize the subset
            var newCol = ColumnFactory.Create<T>(Name, indices.Count, IsNullable);

            for (int i = 0; i < indices.Count; i++)
            {
                int viewIndex = indices[i];
                if (viewIndex < 0 || viewIndex >= _indices.Length) throw new IndexOutOfRangeException();

                int realIndex = _indices[viewIndex];

                if (IsNullable && _source.IsNull(realIndex))
                {
                    newCol.Append(default!);
                    newCol.SetNull(i);
                }
                else
                {
                    newCol.Append(_source.GetValue(realIndex));
                }
            }
            return newCol;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            // We do not own the source, so we do NOT dispose it.
        }

        // --- Aggregation Interface Implementation (Not Supported yet) ---

        /// <summary>
        /// Not supported for IndirectColumn.
        /// </summary>
        /// <param name="indices"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public object? ComputeSum(int[] indices, int start, int end)
            => throw new NotSupportedException($"Aggregation not supported on {GetType().Name}");

        /// <summary>
        /// Not supported for IndirectColumn.
        /// </summary>
        /// <param name="indices"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public object? ComputeMean(int[] indices, int start, int end)
            => throw new NotSupportedException($"Aggregation not supported on {GetType().Name}");

        /// <summary>
        /// Not supported for IndirectColumn.
        /// </summary>
        /// <param name="indices"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public object? ComputeMin(int[] indices, int start, int end)
            => throw new NotSupportedException($"Aggregation not supported on {GetType().Name}");

        /// <summary>
        /// Not supported for IndirectColumn.
        /// </summary>
        /// <param name="indices"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public object? ComputeMax(int[] indices, int start, int end)
            => throw new NotSupportedException($"Aggregation not supported on {GetType().Name}");
    }
}