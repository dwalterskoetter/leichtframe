using System.Runtime.CompilerServices;
using LeichtFrame.Core.Logic;

namespace LeichtFrame.Core
{
    /// <summary>
    /// Reader for fast streaming of Group Key / Count pairs from NativeGroupedData.
    /// </summary>
    public unsafe ref struct NativeGroupCountReader
    {
        private int* _pKey;
        private int* _pNextOffset;
        private readonly int* _pEnd;
        private int _cachedStartOffset;

        internal NativeGroupCountReader(NativeGroupedData data)
        {
            _pKey = data.Keys.Ptr;
            _pEnd = data.Keys.Ptr + data.GroupCount;
            // Caching Logik:
            _cachedStartOffset = data.Offsets.Ptr[0];
            _pNextOffset = data.Offsets.Ptr + 1;
        }

        /// <summary>
        /// Reads Key and Count in a single CPU step.
        /// Returns false if end is reached.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Read(out int key, out int count)
        {
            // 1. Boundary Check
            if (_pKey >= _pEnd)
            {
                key = 0; count = 0;
                return false;
            }

            // 2. Fetch Data (Pointer Increment)
            key = *_pKey;
            _pKey++;

            int nextOffset = *_pNextOffset;
            _pNextOffset++;

            // 3. Calc Count (Register Math)
            count = nextOffset - _cachedStartOffset;
            _cachedStartOffset = nextOffset;

            return true;
        }
    }

    /// <summary>
    /// Extension Methods for GroupedDataFrame to access NativeGroupCountReader
    /// </summary>
    public static class GroupStreamingExtensions
    {
        /// <summary>
        /// Gets a NativeGroupCountReader for fast streaming of Group Key / Count pairs.
        /// </summary>
        /// <param name="gdf"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        public static NativeGroupCountReader GetCountReader(this GroupedDataFrame gdf)
        {
            if (gdf.NativeData == null) throw new InvalidOperationException("Slow Path!");
            return new NativeGroupCountReader(gdf.NativeData);
        }
    }
}