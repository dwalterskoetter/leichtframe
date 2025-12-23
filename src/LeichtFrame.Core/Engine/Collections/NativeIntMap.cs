using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace LeichtFrame.Core.Engine.Collections
{
    internal unsafe struct NativeIntMap : IDisposable
    {
        private byte* _ctrl;

        private int* _keys;
        private int* _groupIds;

        private int _capacity;
        private int _mask;
        private int _count;
        private int _resizeThreshold;

        private const byte Empty = 0;
        private const int GroupSize = 32;

        public int Count => _count;
        public int Capacity => _capacity;

        public NativeIntMap(int initialCapacity)
        {
            _capacity = NextPowerOfTwo(Math.Max(GroupSize, initialCapacity));
            _mask = _capacity - 1;
            _resizeThreshold = (int)(_capacity * 0.75f);
            _count = 0;

            nuint sizeCtrl = (nuint)(_capacity + GroupSize);
            nuint sizeData = (nuint)(_capacity * sizeof(int));

            _ctrl = (byte*)NativeMemory.AllocZeroed(sizeCtrl);

            _keys = (int*)NativeMemory.Alloc(sizeData);
            _groupIds = (int*)NativeMemory.Alloc(sizeData);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetOrAdd(int key)
        {
            if (_count >= _resizeThreshold) Resize();

            int hash = Hash(key);

            byte h2 = (byte)((hash & 0x7F) + 1);

            int idx = hash & _mask;

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
                        _ctrl[realIdx] = h2;
                        _keys[realIdx] = key;

                        int newGroupId = _count;
                        _groupIds[realIdx] = newGroupId;
                        _count++;

                        if (realIdx < GroupSize)
                        {
                            _ctrl[realIdx + _capacity] = h2;
                        }

                        return newGroupId;
                    }
                    else if (foundCtrl == h2)
                    {
                        if (_keys[realIdx] == key)
                        {
                            return _groupIds[realIdx];
                        }
                    }

                    mask &= ~(1 << bitPos);
                }

                idx += GroupSize;

                if (idx >= _capacity)
                {
                    idx -= _capacity;
                }
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void Resize()
        {
            int oldCap = _capacity;
            int newCap = oldCap * 2;
            int newMask = newCap - 1;

            byte* oldCtrl = _ctrl;
            int* oldKeys = _keys;
            int* oldIds = _groupIds;

            nuint sizeCtrl = (nuint)(newCap + GroupSize);
            nuint sizeData = (nuint)(newCap * sizeof(int));

            _ctrl = (byte*)NativeMemory.AllocZeroed(sizeCtrl);
            _keys = (int*)NativeMemory.Alloc(sizeData);
            _groupIds = (int*)NativeMemory.Alloc(sizeData);

            _capacity = newCap;
            _mask = newMask;
            _resizeThreshold = (int)(newCap * 0.75f);

            for (int i = 0; i < oldCap; i++)
            {
                if (oldCtrl[i] != Empty)
                {
                    int key = oldKeys[i];
                    int id = oldIds[i];

                    int hash = Hash(key);
                    byte h2 = (byte)((hash & 0x7F) + 1);
                    int idx = hash & newMask;

                    while (true)
                    {
                        if (_ctrl[idx] == Empty)
                        {
                            _ctrl[idx] = h2;
                            _keys[idx] = key;
                            _groupIds[idx] = id;

                            if (idx < GroupSize) _ctrl[idx + newCap] = h2;

                            break;
                        }
                        idx = (idx + 1) & newMask;
                    }
                }
            }

            NativeMemory.Free(oldCtrl);
            NativeMemory.Free(oldKeys);
            NativeMemory.Free(oldIds);
        }

        public int[] ExportKeys()
        {
            var result = new int[_count];
            fixed (int* pRes = result)
            {
                for (int i = 0; i < _capacity; i++)
                {
                    if (_ctrl[i] != Empty)
                    {
                        int id = _groupIds[i];
                        pRes[id] = _keys[i];
                    }
                }
            }
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int Hash(int k)
        {
            k ^= k >> 16;
            k *= unchecked((int)0x85ebca6b);
            k ^= k >> 13;
            k *= unchecked((int)0xc2b2ae35);
            k ^= k >> 16;
            return k;
        }

        private static int NextPowerOfTwo(int x)
        {
            return (int)BitOperations.RoundUpToPowerOf2((uint)x);
        }

        public void Dispose()
        {
            if (_ctrl != null) { NativeMemory.Free(_ctrl); _ctrl = null; }
            if (_keys != null) { NativeMemory.Free(_keys); _keys = null; }
            if (_groupIds != null) { NativeMemory.Free(_groupIds); _groupIds = null; }
        }
    }
}