using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace LeichtFrame.Core.Engine.Collections
{
    /// <summary>
    /// Specialized Swiss Table for Fixed-Width Byte Rows (Multi-Column Keys).
    /// </summary>
    internal unsafe struct NativeRowMap : IDisposable
    {
        private byte* _ctrl;
        private int* _rowIndices;
        private int* _groupIds;

        private int _capacity;
        private int _mask;
        private int _count;
        private int _resizeThreshold;

        private readonly byte* _packedRows;
        private readonly int _rowWidthBytes;

        private const byte Empty = 0;
        private const int GroupSize = 32;

        public int Count => _count;

        public NativeRowMap(int initialCapacity, byte* packedRows, int rowWidthBytes)
        {
            _capacity = NextPowerOfTwo(Math.Max(GroupSize, initialCapacity));
            _mask = _capacity - 1;
            _resizeThreshold = (int)(_capacity * 0.75f);
            _count = 0;
            _packedRows = packedRows;
            _rowWidthBytes = rowWidthBytes;

            AllocateMemory(_capacity);
        }

        private void AllocateMemory(int cap)
        {
            _ctrl = (byte*)NativeMemory.AllocZeroed((nuint)(cap + GroupSize));
            _rowIndices = (int*)NativeMemory.Alloc((nuint)(cap * sizeof(int)));
            _groupIds = (int*)NativeMemory.Alloc((nuint)(cap * sizeof(int)));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetOrAdd(int rowIndex, int hash)
        {
            if (_count >= _resizeThreshold) Resize();

            byte h2 = (byte)((hash & 0x7F) + 1);
            int idx = hash & _mask;

            byte* pKey = _packedRows + (rowIndex * _rowWidthBytes);

            while (true)
            {
                Vector256<byte> ctrlGroup = Vector256.Load(_ctrl + idx);
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
                        _ctrl[realIdx] = h2;
                        _rowIndices[realIdx] = rowIndex;
                        int gid = _count++;
                        _groupIds[realIdx] = gid;

                        if (realIdx < GroupSize) _ctrl[realIdx + _capacity] = h2;
                        return gid;
                    }
                    else
                    {
                        int existingRowIdx = _rowIndices[realIdx];
                        byte* pExisting = _packedRows + (existingRowIdx * _rowWidthBytes);

                        if (RowEquals(pKey, pExisting, _rowWidthBytes))
                        {
                            return _groupIds[realIdx];
                        }
                    }
                    mask &= ~(1 << bitPos);
                }
                idx += GroupSize;
                if (idx >= _capacity) idx -= _capacity;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool RowEquals(byte* a, byte* b, int len)
        {
            return new ReadOnlySpan<byte>(a, len).SequenceEqual(new ReadOnlySpan<byte>(b, len));
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void Resize()
        {
            int oldCap = _capacity;
            byte* oldCtrl = _ctrl;
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
                    int hash = RecomputeHash(rowIdx);
                    byte h2 = oldCtrl[i];
                    int idx = hash & _mask;

                    while (true)
                    {
                        if (_ctrl[idx] == Empty)
                        {
                            _ctrl[idx] = h2;
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
            NativeMemory.Free(oldRowIndices);
            NativeMemory.Free(oldGroupIds);
        }

        private int RecomputeHash(int rowIdx)
        {
            byte* pRow = _packedRows + (rowIdx * _rowWidthBytes);
            int hash = unchecked((int)2166136261);
            for (int i = 0; i < _rowWidthBytes; i++)
            {
                hash = (hash ^ pRow[i]) * 16777619;
            }
            return hash;
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

        private static int NextPowerOfTwo(int x) => (int)BitOperations.RoundUpToPowerOf2((uint)x);

        public void Dispose()
        {
            if (_ctrl != null) { NativeMemory.Free(_ctrl); _ctrl = null; }
            if (_rowIndices != null) { NativeMemory.Free(_rowIndices); _rowIndices = null; }
            if (_groupIds != null) { NativeMemory.Free(_groupIds); _groupIds = null; }
        }
    }
}