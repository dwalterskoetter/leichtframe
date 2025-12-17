using System.Runtime.InteropServices;
using LeichtFrame.Core.Logic;

namespace LeichtFrame.Core
{
    /// <summary>
    /// Represents the result of a GroupBy operation.
    /// This abstract base class holds references to the source data and manages the lifecycle of native resources.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Performance Architecture:</b><br/>
    /// This class operates in two modes:
    /// <list type="number">
    /// <item>
    ///     <description><b>Native Mode (Fast Path):</b> Uses <see cref="NativeData"/> (unmanaged memory) to store indices and offsets. 
    ///     Aggregations like <c>.Count()</c> or <c>.Sum()</c> access this directly without allocation (Zero-Copy).</description>
    /// </item>
    /// <item>
    ///     <description><b>Managed Mode (Fallback):</b> Stores data in standard C# arrays. Used for string grouping or complex types.</description>
    /// </item>
    /// </list>
    /// </para>
    /// </remarks>
    public abstract class GroupedDataFrame : IDisposable
    {
        /// <summary>
        /// Gets the source DataFrame that was grouped.
        /// </summary>
        public DataFrame Source { get; }

        /// <summary>
        /// Gets the name of the column used for grouping.
        /// </summary>
        public string GroupColumnName { get; }

        /// <summary>
        /// Internal reference to unmanaged memory containing the grouping results (CSR format).
        /// <br/>
        /// If this is not null, the "Fast Path" is active.
        /// </summary>
        internal NativeGroupedData? NativeData { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="GroupedDataFrame"/> class.
        /// </summary>
        /// <param name="source">The source DataFrame.</param>
        /// <param name="groupColumnName">The grouping column name.</param>
        /// <param name="nativeData">Optional pointer to native data. Must be internal to match the visibility of NativeGroupedData.</param>
        internal GroupedDataFrame(DataFrame source, string groupColumnName, NativeGroupedData? nativeData)
        {
            Source = source;
            GroupColumnName = groupColumnName;
            NativeData = nativeData;
        }

        /// <summary>
        /// Gets the total number of groups found.
        /// </summary>
        public abstract int GroupCount { get; }

        /// <summary>
        /// Gets the group offsets for Compressed Sparse Row (CSR) iteration.
        /// </summary>
        /// <remarks>
        /// <b>Performance Warning:</b><br/>
        /// If this instance is in <b>Native Mode</b>, accessing this property triggers a 
        /// <see cref="Marshal.Copy(IntPtr, int[], int, int)"/>. This allocates a new managed array and copies memory.
        /// <br/>
        /// For high performance, use the aggregation extension methods (.Sum, .Count) which bypass this property.
        /// </remarks>
        public abstract int[] GroupOffsets { get; }

        /// <summary>
        /// Gets the row indices sorted by group (CSR Values).
        /// </summary>
        /// <remarks>
        /// <b>Performance Warning:</b><br/>
        /// If this instance is in <b>Native Mode</b>, accessing this property triggers a 
        /// <see cref="Marshal.Copy(IntPtr, int[], int, int)"/>. This allocates a new managed array (size of RowCount) and copies memory.
        /// </remarks>
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

        /// <summary>
        /// Releases unmanaged resources (NativeGroupedData) if they exist.
        /// </summary>
        public virtual void Dispose()
        {
            NativeData?.Dispose();
            NativeData = null;
        }
    }

    /// <summary>
    /// A strongly-typed implementation of <see cref="GroupedDataFrame"/> for a specific key type.
    /// </summary>
    /// <typeparam name="TKey">The type of the grouping key (e.g., int, string).</typeparam>
    public class GroupedDataFrame<TKey> : GroupedDataFrame
    {
        // Cache fields for Managed Fallback or Lazy Materialization
        private TKey[]? _keys;
        private int[]? _groupOffsets;
        private int[]? _rowIndices;
        private int[]? _nullGroupIndices;

        /// <summary>
        /// <b>FAST PATH CONSTRUCTOR.</b>
        /// Initializes a new instance using unmanaged memory.
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="colName">The column name.</param>
        /// <param name="nativeData">The unmanaged data structure holding indices and offsets.</param>
        internal GroupedDataFrame(DataFrame df, string colName, NativeGroupedData nativeData)
            : base(df, colName, nativeData)
        {
        }

        /// <summary>
        /// <b>SLOW PATH CONSTRUCTOR.</b>
        /// Initializes a new instance using managed arrays.
        /// </summary>
        /// <param name="df">The source DataFrame.</param>
        /// <param name="colName">The column name.</param>
        /// <param name="keys">The group keys.</param>
        /// <param name="offsets">The CSR offsets.</param>
        /// <param name="indices">The CSR row indices.</param>
        /// <param name="nullIndices">Optional indices for null values.</param>
        public GroupedDataFrame(DataFrame df, string colName, TKey[] keys, int[] offsets, int[] indices, int[]? nullIndices)
            : base(df, colName, null)
        {
            _keys = keys;
            _groupOffsets = offsets;
            _rowIndices = indices;
            _nullGroupIndices = nullIndices;
        }

        /// <inheritdoc />
        public override int GroupCount => NativeData?.GroupCount ?? _keys!.Length;

        /// <inheritdoc cref="GroupedDataFrame.GroupOffsets"/>
        public override int[] GroupOffsets
        {
            get
            {
                // Lazy Loading: Wir erstellen das Array nur, wenn jemand es wirklich haben will.
                if (_groupOffsets == null && NativeData != null)
                {
                    _groupOffsets = new int[NativeData.GroupCount + 1];

                    // UNSAFE: Notwendig für den Zugriff auf den Pointer
                    unsafe
                    {
                        // Performance Hit: Hier wird kopiert!
                        Marshal.Copy((nint)NativeData.Offsets.Ptr, _groupOffsets, 0, NativeData.GroupCount + 1);
                    }
                }
                return _groupOffsets!;
            }
        }

        /// <inheritdoc cref="GroupedDataFrame.RowIndices"/>
        public override int[] RowIndices
        {
            get
            {
                // Lazy Loading
                if (_rowIndices == null && NativeData != null)
                {
                    _rowIndices = new int[NativeData.RowCount];

                    unsafe
                    {
                        // Performance Hit: Hier wird kopiert (O(N) Allocation)!
                        Marshal.Copy((nint)NativeData.Indices.Ptr, _rowIndices, 0, NativeData.RowCount);
                    }
                }
                return _rowIndices!;
            }
        }

        /// <inheritdoc />
        public override int[]? NullGroupIndices => _nullGroupIndices;

        /// <summary>
        /// Returns the keys array. Lazily materializes from native memory if required.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown if NativeData is present but TKey is not int.</exception>
        public override Array GetKeys()
        {
            if (_keys == null && NativeData != null)
            {
                // Native Mapping: Aktuell unterstützt die Engine nur INT für Native Grouping.
                if (typeof(TKey) == typeof(int))
                {
                    int[] intKeys = new int[NativeData.GroupCount];
                    unsafe
                    {
                        Marshal.Copy((nint)NativeData.Keys.Ptr, intKeys, 0, NativeData.GroupCount);
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