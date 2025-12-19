using System.Runtime.CompilerServices;

namespace LeichtFrame.Core
{
    /// <summary>
    /// Extension methods for streaming aggregations.
    /// </summary>
    public static class StreamingAggregationExtensions
    {
        /// <summary>
        /// Provides a zero-allocation streaming enumerator over group key/count pairs.
        /// Enables high-performance aggregation scenarios.
        /// </summary>
        public static GroupCountStream CountStream(this GroupedDataFrame gdf)
        {
            return new GroupCountStream(gdf);
        }
    }

    /// <summary>
    /// The "Enumerable" (Struct, to avoid allocation).
    /// </summary>
    public readonly struct GroupCountStream
    {
        private readonly GroupedDataFrame _gdf;

        internal GroupCountStream(GroupedDataFrame gdf)
        {
            _gdf = gdf;
        }

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        /// <returns></returns>
        public GroupCountEnumerator GetEnumerator() => new GroupCountEnumerator(_gdf);
    }

    /// <summary>
    /// The "Enumerator" (Ref Struct). 
    /// Wrapper for NativeGroupCountReader.
    /// </summary>
    public ref struct GroupCountEnumerator
    {
        private NativeGroupCountReader _reader;
        private readonly GroupedDataFrame _gdf;
        private int _currentKey;
        private int _currentCount;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal GroupCountEnumerator(GroupedDataFrame gdf)
        {
            _gdf = gdf;
            _reader = gdf.GetCountReader();
            _currentKey = 0;
            _currentCount = 0;
        }

        /// <summary>
        /// Advances to the next key/count pair.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
            return _reader.Read(out _currentKey, out _currentCount);
        }
        /// <summary>
        /// Gets the current key/count pair.
        /// </summary>
        public (int Key, int Count) Current
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (_currentKey, _currentCount);
        }

        /// <summary>
        /// Disposes the enumerator.
        /// </summary>
        public void Dispose()
        {
            _gdf.Dispose();
        }
    }
}