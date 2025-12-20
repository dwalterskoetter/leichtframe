using System.Runtime.InteropServices;
using LeichtFrame.Core.Engine;

namespace LeichtFrame.Core
{
    /// <summary>
    /// Represents the result of a GroupBy operation.
    /// This abstract base class holds references to the source data and manages the lifecycle of native resources.
    /// </summary>
    public abstract class GroupedDataFrame : IDisposable
    {
        /// <summary>
        /// Gets the source DataFrame that was grouped.
        /// </summary>
        public DataFrame Source { get; }

        /// <summary>
        /// Gets the names of the columns used for grouping.
        /// </summary>
        public string[] GroupColumnNames { get; }

        /// <summary>
        /// Internal reference to unmanaged memory containing the grouping results (CSR format).
        /// If this is not null, the "Fast Path" is active.
        /// </summary>
        internal NativeGroupedData? NativeData { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="GroupedDataFrame"/> class.
        /// </summary>
        /// <param name="source">The source DataFrame.</param>
        /// <param name="groupColumnNames">The names of the grouping columns.</param>
        /// <param name="nativeData">Optional pointer to native data.</param>
        internal GroupedDataFrame(DataFrame source, string[] groupColumnNames, NativeGroupedData? nativeData)
        {
            Source = source;
            GroupColumnNames = groupColumnNames;
            NativeData = nativeData;
        }

        /// <summary>
        /// Gets the total number of groups found.
        /// </summary>
        public abstract int GroupCount { get; }

        /// <summary>
        /// Gets the group offsets for Compressed Sparse Row (CSR) iteration.
        /// </summary>
        public abstract int[] GroupOffsets { get; }

        /// <summary>
        /// Gets the row indices sorted by group (CSR Values).
        /// </summary>
        public abstract int[] RowIndices { get; }

        /// <summary>
        /// Gets the indices of rows belonging to the 'null' group, if any.
        /// </summary>
        public abstract int[]? NullGroupIndices { get; }

        /// <summary>
        /// Gets the unique keys of the groups as a generic Array.
        /// </summary>
        /// <returns>An array containing the group keys.</returns>
        public abstract Array GetKeys();

        // -----------------------------------------------------------
        // KORREKTUR: Methode hierher verschoben (in die Basisklasse)
        // -----------------------------------------------------------

        /// <summary>
        /// Creates a shallow copy of this GroupedDataFrame attached to a new Source DataFrame.
        /// Used internally when injecting computed columns.
        /// </summary>
        internal GroupedDataFrame WithSource(DataFrame newSource)
        {
            // Case 1: Integer Keys (Fast Path or Slow Path)
            if (this is GroupedDataFrame<int> gInt)
            {
                if (NativeData != null)
                {
                    return new GroupedDataFrame<int>(newSource, GroupColumnNames, NativeData);
                }

                return new GroupedDataFrame<int>(
                    newSource,
                    GroupColumnNames,
                    (int[])gInt.GetKeys(),
                    gInt.GroupOffsets,
                    gInt.RowIndices,
                    gInt.NullGroupIndices
                );
            }

            // Case 2: String Keys
            if (this is GroupedDataFrame<string> gStr)
            {
                return new GroupedDataFrame<string>(
                    newSource,
                    GroupColumnNames,
                    (string[])gStr.GetKeys(),
                    gStr.GroupOffsets,
                    gStr.RowIndices,
                    gStr.NullGroupIndices
                );
            }

            throw new NotSupportedException($"Re-binding source not supported for key type of {this.GetType().Name}");
        }

        /// <summary>
        /// Releases unmanaged resources (NativeGroupedData) if they exist.
        /// </summary>
        public virtual void Dispose()
        {
            NativeData?.Dispose();
            NativeData = null;
            GC.SuppressFinalize(this);
        }
    }

    /// <summary>
    /// A strongly-typed implementation of <see cref="GroupedDataFrame"/> for a specific key type.
    /// Primarily used for single-column grouping strategies.
    /// </summary>
    /// <typeparam name="TKey">The type of the grouping key (e.g., int, string).</typeparam>
    public class GroupedDataFrame<TKey> : GroupedDataFrame
    {
        private TKey[]? _keys;
        private int[]? _groupOffsets;
        private int[]? _rowIndices;
        private readonly int[]? _nullGroupIndices;

        /// <summary>
        /// Initializes a new instance using unmanaged memory (Fast Path).
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="colNames">The column names.</param>
        /// <param name="nativeData">The unmanaged data structure.</param>
        internal GroupedDataFrame(DataFrame df, string[] colNames, NativeGroupedData nativeData)
            : base(df, colNames, nativeData)
        {
        }

        /// <summary>
        /// Initializes a new instance using managed arrays (Slow Path / Fallback).
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="colNames">The column names.</param>
        /// <param name="keys">The group keys.</param>
        /// <param name="offsets">The CSR offsets.</param>
        /// <param name="indices">The CSR row indices.</param>
        /// <param name="nullIndices">Optional indices for null values.</param>
        public GroupedDataFrame(DataFrame df, string[] colNames, TKey[] keys, int[] offsets, int[] indices, int[]? nullIndices)
            : base(df, colNames, null)
        {
            _keys = keys;
            _groupOffsets = offsets;
            _rowIndices = indices;
            _nullGroupIndices = nullIndices;
        }

        /// <inheritdoc />
        public override int GroupCount => NativeData?.GroupCount ?? _keys!.Length;

        /// <inheritdoc />
        public override int[] GroupOffsets
        {
            get
            {
                // Lazy loading: Copy from native memory only if requested as managed array.
                if (_groupOffsets == null && NativeData != null)
                {
                    int count = NativeData.GroupCount + 1;
                    _groupOffsets = new int[count];
                    unsafe
                    {
                        Marshal.Copy((nint)NativeData.Offsets.Ptr, _groupOffsets, 0, count);
                    }
                }
                return _groupOffsets!;
            }
        }

        /// <inheritdoc />
        public override int[] RowIndices
        {
            get
            {
                // Lazy loading: Copy from native memory only if requested.
                if (_rowIndices == null && NativeData != null)
                {
                    int count = NativeData.RowCount;
                    _rowIndices = new int[count];
                    unsafe
                    {
                        Marshal.Copy((nint)NativeData.Indices.Ptr, _rowIndices, 0, count);
                    }
                }
                return _rowIndices!;
            }
        }

        /// <inheritdoc />
        public override int[]? NullGroupIndices => _nullGroupIndices;

        /// <inheritdoc />
        public override Array GetKeys()
        {
            if (_keys == null && NativeData != null)
            {
                if (typeof(TKey) == typeof(int))
                {
                    int count = NativeData.GroupCount;
                    int[] intKeys = new int[count];
                    unsafe
                    {
                        Marshal.Copy((nint)NativeData.Keys.Ptr, intKeys, 0, count);
                    }
                    _keys = (TKey[])(object)intKeys;
                }
                else
                {
                    throw new InvalidOperationException($"NativeData is present, but generic type {typeof(TKey).Name} is not supported for native extraction.");
                }
            }
            return _keys!;
        }
    }
}