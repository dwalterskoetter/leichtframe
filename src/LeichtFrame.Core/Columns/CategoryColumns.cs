namespace LeichtFrame.Core
{
    /// <summary>
    /// A high-performance column optimized for low-cardinality string data (Categorical).
    /// Stores data internally as integer codes referencing a shared dictionary.
    /// </summary>
    public class CategoryColumn : Column<string?>, IDisposable
    {
        private readonly IntColumn _codes;

        private readonly List<string?> _dictionary;
        internal List<string?> InternalDictionary => _dictionary;
        private readonly Dictionary<string, int> _lookup;

        /// <summary>
        /// Initializes a new instance of the <see cref="CategoryColumn"/> class.
        /// </summary>
        public CategoryColumn(string name, int capacity = 16) : base(name, true)
        {
            _codes = new IntColumn(name + "_codes", capacity, isNullable: true);
            _dictionary = new List<string?> { null };
            _lookup = new Dictionary<string, int>();
        }

        private CategoryColumn(string name, IntColumn codes, List<string?> dictionary, Dictionary<string, int> lookup)
            : base(name, true)
        {
            _codes = codes;
            _dictionary = dictionary;
            _lookup = lookup;
        }

        /// <summary>Gets the internal integer codes column.</summary>
        internal IntColumn Codes => _codes;

        /// <summary>Gets the dictionary array mapping codes to strings.</summary>
        internal string?[] DictionaryArray => _dictionary.ToArray();

        /// <summary>Gets the number of unique categories.</summary>
        internal int Cardinality => _dictionary.Count;

        /// <inheritdoc />
        public override int Length => _codes.Length;

        /// <inheritdoc />
        public override ReadOnlyMemory<string?> Values => throw new NotSupportedException("Use Get() or Codes.");

        /// <inheritdoc />
        public override string? Get(int index)
        {
            if (_codes.IsNull(index)) return null;
            return _dictionary[_codes.Get(index)];
        }

        /// <inheritdoc />
        public override void SetValue(int index, string? value) => throw new NotSupportedException("Append-only.");

        /// <inheritdoc />
        public override void Append(string? value)
        {
            if (value == null)
            {
                _codes.Append(null);
                return;
            }

            if (!_lookup.TryGetValue(value, out int code))
            {
                code = _dictionary.Count;
                _dictionary.Add(value);
                _lookup[value] = code;
            }
            _codes.Append(code);
        }

        /// <inheritdoc />
        public override bool IsNull(int index) => _codes.IsNull(index);
        /// <inheritdoc />
        public override void SetNull(int index) => _codes.SetNull(index);
        /// <inheritdoc />
        public override void SetNotNull(int index) => _codes.SetNotNull(index);
        /// <inheritdoc />
        public override void EnsureCapacity(int minCapacity) => _codes.EnsureCapacity(minCapacity);

        /// <inheritdoc />
        public override IColumn CloneSubset(IReadOnlyList<int> indices)
        {
            var newCodes = (IntColumn)_codes.CloneSubset(indices);
            return new CategoryColumn(Name, newCodes, _dictionary, _lookup);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _codes.Dispose();
        }

        internal static CategoryColumn CreateFromInternals(string name, int[] codes, int length, List<string?> dictionary, bool isPooledCodes = false)
        {
            var lookup = new Dictionary<string, int>(dictionary.Count);
            for (int i = 1; i < dictionary.Count; i++)
            {
                if (dictionary[i] != null) lookup[dictionary[i]!] = i;
            }

            var intCol = new IntColumn(name + "_codes", codes, length, deriveNullsFromZero: true, isPooled: isPooledCodes);

            return new CategoryColumn(name, intCol, dictionary, lookup);
        }
    }
}