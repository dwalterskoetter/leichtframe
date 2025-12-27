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
            // Optimierter Prefix Load
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

                // SIMD Match Finder: Suche H2 oder Empty
                int mask = Avx2.MoveMask(
                    Vector256.Equals(ctrlGroup, Vector256.Create(h2)) |
                    Vector256.Equals(ctrlGroup, Vector256.Create(Empty))
                );

                while (mask != 0)
                {
                    int bitPos = BitOperations.TrailingZeroCount(mask);
                    int realIdx = (idx + bitPos) & _mask;
                    byte foundCtrl = _ctrl[realIdx];

                    if (foundCtrl == Empty)
                    {
                        // --- INSERT ---
                        _ctrl[realIdx] = h2;
                        _lengths[realIdx] = len;
                        _prefixes[realIdx] = prefix;
                        _rowIndices[realIdx] = rowIndex;

                        int newGroupId = _count;
                        _groupIds[realIdx] = newGroupId;
                        _count++;

                        if (realIdx < GroupSize) _ctrl[realIdx + _capacity] = h2;
                        return newGroupId;
                    }
                    else
                    {
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
            byte* pA = _sourceBytes + _sourceOffsets[rowA];
            byte* pB = _sourceBytes + _sourceOffsets[rowB];

            if (Avx2.IsSupported && len >= 32)
            {
                Vector256<byte> vecA = Avx2.LoadVector256(pA);
                Vector256<byte> vecB = Avx2.LoadVector256(pB);

                int mask = Avx2.MoveMask(Avx2.CompareEqual(vecA, vecB));

                if (mask != -1) return false;

                int offset = len - 32;
                vecA = Avx2.LoadVector256(pA + offset);
                vecB = Avx2.LoadVector256(pB + offset);

                mask = Avx2.MoveMask(Avx2.CompareEqual(vecA, vecB));
                return mask == -1;
            }

            return new ReadOnlySpan<byte>(pA, len).SequenceEqual(new ReadOnlySpan<byte>(pB, len));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ExportRowIndicesTo(int* destination)
        {
            for (int i = 0; i < _capacity; i++)
            {
                if (_ctrl[i] != 0)
                {
                    int id = _groupIds[i];
                    destination[id] = _rowIndices[i];
                }
            }
        }

        public int[] ExportKeysAsRowIndices()
        {
            var result = new int[_count];
            fixed (int* pRes = result)
            {
                ExportRowIndicesTo(pRes);
            }
            return result;
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