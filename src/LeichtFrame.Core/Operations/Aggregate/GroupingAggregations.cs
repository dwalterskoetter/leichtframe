using System.Runtime.CompilerServices;
using LeichtFrame.Core.Engine.Kernels.Aggregate;

namespace LeichtFrame.Core.Operations.Aggregate
{
    /// <summary>
    /// Provides extension methods for performing aggregations and streaming on grouped dataframes.
    /// </summary>
    public static class GroupAggregationExtensions
    {
        // --- Materialization (DataFrame Result) ---

        /// <inheritdoc/>
        public static DataFrame Count(this GroupedDataFrame gdf) => AggregateDispatcher.Count(gdf);

        /// <inheritdoc/>
        public static DataFrame Sum(this GroupedDataFrame gdf, string columnName)
            => AggregateDispatcher.Sum(gdf, columnName);

        /// <inheritdoc/>
        public static DataFrame Min(this GroupedDataFrame gdf, string columnName)
            => AggregateDispatcher.Min(gdf, columnName);

        /// <inheritdoc/>
        public static DataFrame Max(this GroupedDataFrame gdf, string columnName)
            => AggregateDispatcher.Max(gdf, columnName);

        /// <inheritdoc/>
        public static DataFrame Mean(this GroupedDataFrame gdf, string columnName)
            => AggregateDispatcher.Mean(gdf, columnName);

        /// <inheritdoc/>
        public static DataFrame Aggregate(this GroupedDataFrame gdf, params AggregationDef[] aggregations)
            => AggregateDispatcher.Aggregate(gdf, aggregations);

        // --- Streaming (Zero-Alloc Enumerator) ---

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
    /// The "Enumerable" (Struct, to avoid allocation) for Streaming Count.
    /// </summary>
    public readonly struct GroupCountStream
    {
        private readonly GroupedDataFrame _gdf;

        internal GroupCountStream(GroupedDataFrame gdf)
        {
            _gdf = gdf;
        }

        /// <inheritdoc/>
        public GroupCountEnumerator GetEnumerator() => new GroupCountEnumerator(_gdf);
    }

    /// <summary>
    /// The "Enumerator" (Ref Struct) for Streaming Count.
    /// Wrapper for NativeGroupCountReader.
    /// </summary>
    public ref struct GroupCountEnumerator
    {
        private NativeGroupCountEnumerator _reader;
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

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
            return _reader.Read(out _currentKey, out _currentCount);
        }

        /// <inheritdoc/>
        public (int Key, int Count) Current
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (_currentKey, _currentCount);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
        }
    }
}