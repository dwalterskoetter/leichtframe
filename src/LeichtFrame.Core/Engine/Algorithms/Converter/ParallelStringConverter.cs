using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace LeichtFrame.Core.Engine.Algorithms.Converter
{
    internal static unsafe class ParallelStringConverter
    {
        private const int MinRowsForParallel = 100_000;

        public static CategoryColumn Convert(StringColumn col)
        {
            if (col.Length < MinRowsForParallel)
            {
                return ConvertSingleThreaded(col);
            }

            int rowCount = col.Length;
            int parallelism = Environment.ProcessorCount;

            int[] finalCodes = new int[rowCount];
            var partitions = Partitioner.Create(0, rowCount, rowCount / parallelism);
            var localResults = new LocalResult[parallelism];

            fixed (byte* pGlobalBytes = col.RawBytes)
            fixed (int* pGlobalOffsets = col.Offsets)
            fixed (int* pFinalCodes = finalCodes)
            fixed (ulong* pNullBitmap = col.InternalNulls?.RawBuffer)
            {
                IntPtr ptrBytes = (IntPtr)pGlobalBytes;
                IntPtr ptrOffsets = (IntPtr)pGlobalOffsets;
                IntPtr ptrCodes = (IntPtr)pFinalCodes;
                IntPtr ptrNulls = (IntPtr)pNullBitmap;

                Parallel.ForEach(partitions, (range, state, idx) =>
                {
                    localResults[idx] = ProcessChunk(
                        range.Item1,
                        range.Item2,
                        (byte*)ptrBytes,
                        (int*)ptrOffsets,
                        (int*)ptrCodes,
                        (ulong*)ptrNulls
                    );
                });
            }

            return MergeAndCreate(col.Name, finalCodes, localResults);
        }

        private static CategoryColumn ConvertSingleThreaded(StringColumn col)
        {
            int[] finalCodes = new int[col.Length];
            var result = new LocalResult[1];

            fixed (byte* pBytes = col.RawBytes)
            fixed (int* pOffsets = col.Offsets)
            fixed (int* pCodes = finalCodes)
            fixed (ulong* pNulls = col.InternalNulls?.RawBuffer)
            {
                result[0] = ProcessChunk(0, col.Length, pBytes, pOffsets, pCodes, pNulls);
            }

            return MergeAndCreate(col.Name, finalCodes, result);
        }

        private static CategoryColumn MergeAndCreate(string name, int[] finalCodes, LocalResult[] results)
        {
            var globalDict = new List<string?> { null };
            var globalLookup = new Dictionary<string, int>();

            int[][] remapTables = new int[results.Length][];

            for (int t = 0; t < results.Length; t++)
            {
                var localList = results[t].LocalDict;
                if (localList == null) { remapTables[t] = Array.Empty<int>(); continue; }

                var map = new int[localList.Count];
                map[0] = 0;

                for (int c = 1; c < localList.Count; c++)
                {
                    string s = localList[c];
                    if (!globalLookup.TryGetValue(s, out int gCode))
                    {
                        gCode = globalDict.Count;
                        globalDict.Add(s);
                        globalLookup[s] = gCode;
                    }
                    map[c] = gCode;
                }
                remapTables[t] = map;
            }

            int rowCount = finalCodes.Length;
            int parallelism = results.Length;

            if (parallelism == 1)
            {
                int[] map = remapTables[0];
                if (map.Length > 0)
                {
                    for (int i = 0; i < rowCount; i++) finalCodes[i] = map[finalCodes[i]];
                }
            }
            else
            {
                int chunkSize = rowCount / parallelism;
                fixed (int* pCodes = finalCodes)
                {
                    IntPtr ptrCodes = (IntPtr)pCodes;
                    Parallel.For(0, parallelism, t =>
                    {
                        int start = t * chunkSize;
                        int end = (t == parallelism - 1) ? rowCount : start + chunkSize;
                        int[] map = remapTables[t];
                        if (map.Length == 0) return;
                        int* ptr = (int*)ptrCodes + start;
                        int* ptrEnd = (int*)ptrCodes + end;
                        while (ptr < ptrEnd) { *ptr = map[*ptr]; ptr++; }
                    });
                }
            }

            return CategoryColumn.CreateFromInternals(name, finalCodes, globalDict);
        }

        private struct LocalResult { public List<string> LocalDict; }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static LocalResult ProcessChunk(int start, int end, byte* pBytes, int* pOffsets, int* pOutCodes, ulong* pNulls)
        {
            var dictList = new List<string> { null! };
            var map = new NativeLocalMap();
            map.Init(1024, pBytes);

            int* ptrOut = pOutCodes + start;

            try
            {
                if (pNulls != null)
                {
                    for (int i = 0; i < (end - start); i++)
                    {
                        int absIndex = start + i;

                        if ((pNulls[absIndex >> 6] & (1UL << (absIndex & 63))) != 0)
                        {
                            *ptrOut = 0;
                            ptrOut++;
                            continue;
                        }

                        int strStart = pOffsets[absIndex];
                        int len = pOffsets[absIndex + 1] - strStart;

                        byte* pStr = pBytes + strStart;
                        int hash = HashFast(pStr, len);

                        int code = map.GetOrAdd(strStart, len, hash, dictList);
                        *ptrOut = code;
                        ptrOut++;
                    }
                }
                else
                {
                    for (int i = 0; i < (end - start); i++)
                    {
                        int absIndex = start + i;
                        int strStart = pOffsets[absIndex];
                        int len = pOffsets[absIndex + 1] - strStart;

                        byte* pStr = pBytes + strStart;
                        int hash = HashFast(pStr, len);

                        int code = map.GetOrAdd(strStart, len, hash, dictList);
                        *ptrOut = code;
                        ptrOut++;
                    }
                }
            }
            finally
            {
                map.Dispose();
            }

            return new LocalResult { LocalDict = dictList };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int HashFast(byte* ptr, int len)
        {
            int hash = unchecked((int)2166136261);
            while (len >= 4) { hash = (hash ^ Unsafe.ReadUnaligned<int>(ptr)) * 16777619; ptr += 4; len -= 4; }
            for (int i = 0; i < len; i++) hash = (hash ^ ptr[i]) * 16777619;
            return hash;
        }
    }

    internal unsafe struct NativeLocalMap : IDisposable
    {
        public int* Buckets;
        public int* KeyOffsets;
        public int* KeyLengths;

        private int _capacity;
        private int _mask;
        private int _resizeThreshold;
        private int _count;
        private byte* _globalBytes;

        public void Init(int initialCapacity, byte* globalBytes)
        {
            _capacity = initialCapacity < 16 ? 16 : (int)System.Numerics.BitOperations.RoundUpToPowerOf2((uint)initialCapacity);
            _mask = _capacity - 1;
            _resizeThreshold = (int)(_capacity * 0.75f);
            _count = 1;
            _globalBytes = globalBytes;

            nuint size = (nuint)(_capacity * sizeof(int));
            Buckets = (int*)NativeMemory.Alloc(size);
            new Span<int>(Buckets, _capacity).Fill(-1);
            KeyOffsets = (int*)NativeMemory.Alloc(size);
            KeyLengths = (int*)NativeMemory.Alloc(size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetOrAdd(int offset, int len, int hash, List<string> stringAccumulator)
        {
            if (_count >= _resizeThreshold) Resize();

            int idx = hash & _mask;
            int* buckets = Buckets;

            while (true)
            {
                int code = buckets[idx];

                if (code == -1)
                {
                    code = _count;
                    KeyOffsets[code] = offset;
                    KeyLengths[code] = len;
                    buckets[idx] = code;
                    stringAccumulator.Add(Encoding.UTF8.GetString(_globalBytes + offset, len));
                    _count++;
                    return code;
                }
                else
                {
                    if (KeyLengths[code] == len)
                    {
                        byte* pInput = _globalBytes + offset;
                        byte* pCand = _globalBytes + KeyOffsets[code];
                        if (new ReadOnlySpan<byte>(pInput, len).SequenceEqual(new ReadOnlySpan<byte>(pCand, len))) return code;
                    }
                }
                idx = (idx + 1) & _mask;
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void Resize()
        {
            int oldCap = _capacity;
            int newCap = oldCap * 2;
            int newMask = newCap - 1;

            KeyOffsets = (int*)NativeMemory.Realloc(KeyOffsets, (nuint)(newCap * sizeof(int)));
            KeyLengths = (int*)NativeMemory.Realloc(KeyLengths, (nuint)(newCap * sizeof(int)));

            int* oldBuckets = Buckets;
            int* newBuckets = (int*)NativeMemory.Alloc((nuint)(newCap * sizeof(int)));
            new Span<int>(newBuckets, newCap).Fill(-1);

            for (int idx = 0; idx < oldCap; idx++)
            {
                int code = oldBuckets[idx];
                if (code != -1)
                {
                    int off = KeyOffsets[code];
                    int len = KeyLengths[code];
                    int h = unchecked((int)2166136261);
                    byte* ptr = _globalBytes + off;
                    int l = len;
                    while (l >= 4) { h = (h ^ Unsafe.ReadUnaligned<int>(ptr)) * 16777619; ptr += 4; l -= 4; }
                    for (int k = 0; k < l; k++) h = (h ^ ptr[k]) * 16777619;

                    int newSlot = h & newMask;
                    while (newBuckets[newSlot] != -1) newSlot = (newSlot + 1) & newMask;
                    newBuckets[newSlot] = code;
                }
            }

            NativeMemory.Free(oldBuckets);
            Buckets = newBuckets;
            _capacity = newCap;
            _mask = newMask;
            _resizeThreshold = (int)(newCap * 0.75f);
        }

        public void Dispose()
        {
            if (Buckets != null) NativeMemory.Free(Buckets);
            if (KeyOffsets != null) NativeMemory.Free(KeyOffsets);
            if (KeyLengths != null) NativeMemory.Free(KeyLengths);
        }
    }
}