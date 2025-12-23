using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace LeichtFrame.Core.Engine.Collections
{
    /// <summary>
    /// Specialized Swiss Table for Strings with "German String" optimization (Prefix caching).
    /// </summary>
    internal unsafe struct NativeStringMap : IDisposable
    {
        private byte* _ctrl;

        // --- German String Cache (Hot Data) ---
        private int* _lengths;
        private int* _prefixes;

        // --- Payload ---
        private int* _rowIndices;
        private int* _groupIds;

        // --- Source Data Access ---
        private readonly byte* _sourceBytes;
        private readonly int* _sourceOffsets;

        private int _capacity;
        private int _mask;
        private int _count;
        private int _resizeThreshold;

        private const byte Empty = 0;
        private const int GroupSize = 32;

        public int Count => _count;

        public int Capacity => _capacity;

        public NativeStringMap(int initialCapacity, byte* sourceBytes, int* sourceOffsets)
        {
            _capacity = NextPowerOfTwo(Math.Max(GroupSize, initialCapacity));
            _mask = _capacity - 1;
            _resizeThreshold = (int)(_capacity * 0.75f);
            _count = 0;

            _sourceBytes = sourceBytes;
            _sourceOffsets = sourceOffsets;

            AllocateMemory(_capacity);
        }

        private void AllocateMemory(int cap)
        {
            nuint sizeCtrl = (nuint)(cap + GroupSize);
            nuint sizeInt = (nuint)(cap * sizeof(int));

            _ctrl = (byte*)NativeMemory.AllocZeroed(sizeCtrl);

            _lengths = (int*)NativeMemory.Alloc(sizeInt);
            _prefixes = (int*)NativeMemory.Alloc(sizeInt);

            _rowIndices = (int*)NativeMemory.Alloc(sizeInt);
            _groupIds = (int*)NativeMemory.Alloc(sizeInt);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetOrAdd(int rowIndex, int hash)
        {
            if (_count >= _resizeThreshold) Resize();

            // 1. Prepare Probe Data
            byte h2 = (byte)((hash & 0x7F) + 1);
            int idx = hash & _mask;

            // 2. Fetch Row Data (Source)
            int start = _sourceOffsets[rowIndex];
            int len = _sourceOffsets[rowIndex + 1] - start;

            int prefix = 0;
            if (len >= 4)
            {
                prefix = Unsafe.ReadUnaligned<int>(_sourceBytes + start);
            }
            else
            {
                byte* pStr = _sourceBytes + start;
                for (int i = 0; i < len; i++) prefix |= (pStr[i] << (i * 8));
            }

            // 3. Probing Loop
            while (true)
            {
                Vector256<byte> ctrlGroup = Vector256.Load(_ctrl + idx);
                Vector256<byte> matchH2 = Vector256.Equals(ctrlGroup, Vector256.Create(h2));
                Vector256<byte> matchEmpty = Vector256.Equals(ctrlGroup, Vector256.Create(Empty));
                int mask = Avx2.MoveMask(matchH2 | matchEmpty);

                while (mask != 0)
                {
                    int bitPos = BitOperations.TrailingZeroCount(mask);
                    int realIdx = (idx + bitPos) & _mask;
                    byte foundCtrl = _ctrl[realIdx];

                    if (foundCtrl == Empty)
                    {
                        // --- INSERT ---
                        _ctrl[realIdx] = h2;

                        // Fill German Cache
                        _lengths[realIdx] = len;
                        _prefixes[realIdx] = prefix;

                        // Fill Payload
                        _rowIndices[realIdx] = rowIndex;
                        int newGroupId = _count;
                        _groupIds[realIdx] = newGroupId;
                        _count++;

                        if (realIdx < GroupSize) _ctrl[realIdx + _capacity] = h2;

                        return newGroupId;
                    }
                    else if (foundCtrl == h2)
                    {
                        // --- MATCH CHECK ---
                        if (_lengths[realIdx] == len && _prefixes[realIdx] == prefix)
                        {
                            if (SeqEqual(rowIndex, _rowIndices[realIdx], len))
                            {
                                return _groupIds[realIdx];
                            }
                        }
                    }

                    mask &= ~(1 << bitPos);
                }

                idx += GroupSize;
                if (idx >= _capacity) idx -= _capacity;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool SeqEqual(int rowA, int rowB, int len)
        {
            int startA = _sourceOffsets[rowA];
            int startB = _sourceOffsets[rowB];

            var spanA = new ReadOnlySpan<byte>(_sourceBytes + startA, len);
            var spanB = new ReadOnlySpan<byte>(_sourceBytes + startB, len);
            return spanA.SequenceEqual(spanB);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void Resize()
        {
            int oldCap = _capacity;
            byte* oldCtrl = _ctrl;
            int* oldLengths = _lengths;
            int* oldPrefixes = _prefixes;
            int* oldRowIndices = _rowIndices;
            int* oldGroupIds = _groupIds;

            int newCap = oldCap * 2;
            AllocateMemory(newCap);

            _capacity = newCap;
            _mask = newCap - 1;
            _resizeThreshold = (int)(newCap * 0.75f);

            for (int i = 0; i < oldCap; i++)
            {
                if (oldCtrl[i] != Empty)
                {
                    int rowIdx = oldRowIndices[i];
                    int len = oldLengths[i];

                    int hash = ComputeHashForResize(rowIdx, len);
                    byte h2 = oldCtrl[i];

                    int idx = hash & _mask;

                    while (true)
                    {
                        if (_ctrl[idx] == Empty)
                        {
                            _ctrl[idx] = h2;
                            _lengths[idx] = len;
                            _prefixes[idx] = oldPrefixes[i];
                            _rowIndices[idx] = rowIdx;
                            _groupIds[idx] = oldGroupIds[i];

                            if (idx < GroupSize) _ctrl[idx + newCap] = h2;
                            break;
                        }
                        idx = (idx + 1) & _mask;
                    }
                }
            }

            NativeMemory.Free(oldCtrl);
            NativeMemory.Free(oldLengths);
            NativeMemory.Free(oldPrefixes);
            NativeMemory.Free(oldRowIndices);
            NativeMemory.Free(oldGroupIds);
        }

        private int ComputeHashForResize(int rowIndex, int len)
        {
            // Simple FNV-1a recalculation locally
            int start = _sourceOffsets[rowIndex];
            int hash = unchecked((int)2166136261);
            byte* pStr = _sourceBytes + start;
            for (int k = 0; k < len; k++)
            {
                hash ^= pStr[k];
                hash *= 16777619;
            }
            return hash;
        }

        public int[] ExportKeysAsRowIndices()
        {
            var result = new int[_count];
            fixed (int* pRes = result)
            {
                for (int i = 0; i < _capacity; i++)
                {
                    if (_ctrl[i] != Empty)
                    {
                        int id = _groupIds[i];
                        pRes[id] = _rowIndices[i];
                    }
                }
            }
            return result;
        }

        private static int NextPowerOfTwo(int x) => (int)BitOperations.RoundUpToPowerOf2((uint)x);

        public void Dispose()
        {
            if (_ctrl != null) { NativeMemory.Free(_ctrl); _ctrl = null; }
            if (_lengths != null) { NativeMemory.Free(_lengths); _lengths = null; }
            if (_prefixes != null) { NativeMemory.Free(_prefixes); _prefixes = null; }
            if (_rowIndices != null) { NativeMemory.Free(_rowIndices); _rowIndices = null; }
            if (_groupIds != null) { NativeMemory.Free(_groupIds); _groupIds = null; }
        }
    }
}