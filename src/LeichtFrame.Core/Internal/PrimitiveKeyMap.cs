using System.Runtime.CompilerServices;

namespace LeichtFrame.Core.Internal
{
    internal sealed class PrimitiveKeyMap<T> : IDisposable where T : unmanaged, IEquatable<T>
    {
        private int[] _buckets;
        private Entry[] _entries;
        private int _count;
        private int _freeList;

        private int[] _groupHeads;
        private int[] _groupTails;
        private int[] _groupCounts;

        public int[] RowNext;

        private struct Entry
        {
            public int HashCode;
            public int Next;
            public T Key;
        }

        public int Count => _count;

        public PrimitiveKeyMap(int capacity, int rowCount)
        {
            int size = GetPrime(capacity);
            _buckets = new int[size];
            Array.Fill(_buckets, -1);

            _entries = new Entry[size];
            _freeList = -1;
            _count = 0;

            _groupHeads = new int[size];
            Array.Fill(_groupHeads, -1);

            _groupTails = new int[size];
            Array.Fill(_groupTails, -1);

            _groupCounts = new int[size];

            RowNext = new int[rowCount];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AddRow(T key, int rowIndex)
        {
            int groupIndex = GetOrAddGroup(key);

            int tail = _groupTails[groupIndex];
            if (tail == -1) _groupHeads[groupIndex] = rowIndex;
            else RowNext[tail] = rowIndex;

            _groupTails[groupIndex] = rowIndex;
            RowNext[rowIndex] = -1;
            _groupCounts[groupIndex]++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetOrAddGroup(T key)
        {
            int hashCode = key.GetHashCode() & 0x7FFFFFFF;
            int bucket = hashCode % _buckets.Length;

            for (int i = _buckets[bucket]; i >= 0; i = _entries[i].Next)
            {
                if (_entries[i].HashCode == hashCode && _entries[i].Key.Equals(key)) return i;
            }

            int index;
            if (_freeList >= 0) { index = _freeList; _freeList = _entries[index].Next; }
            else
            {
                if (_count == _entries.Length) { Resize(); bucket = hashCode % _buckets.Length; }
                index = _count;
                _count++;
            }

            ref Entry entry = ref _entries[index];
            entry.HashCode = hashCode;
            entry.Next = _buckets[bucket];
            entry.Key = key;
            _buckets[bucket] = index;

            return index;
        }

        private void Resize()
        {
            int newSize = GetPrime(_count * 2);
            int[] newBuckets = new int[newSize];
            Array.Fill(newBuckets, -1);
            Entry[] newEntries = new Entry[newSize];
            Array.Copy(_entries, newEntries, _count);

            for (int i = 0; i < _count; i++)
            {
                int bucket = newEntries[i].HashCode % newSize;
                newEntries[i].Next = newBuckets[bucket];
                newBuckets[bucket] = i;
            }
            _buckets = newBuckets;
            _entries = newEntries;

            Array.Resize(ref _groupHeads, newSize);
            for (int i = _count; i < newSize; i++) _groupHeads[i] = -1;
            Array.Resize(ref _groupTails, newSize);
            for (int i = _count; i < newSize; i++) _groupTails[i] = -1;
            Array.Resize(ref _groupCounts, newSize);
        }

        public (T[] Keys, int[] GroupOffsets, int[] RowIndices) ToCSR()
        {
            var keys = new T[_count];
            var offsets = new int[_count + 1];
            int totalRows = 0;
            for (int i = 0; i < _count; i++) totalRows += _groupCounts[i];

            var indices = new int[totalRows];
            int currentOffset = 0;

            for (int i = 0; i < _count; i++)
            {
                keys[i] = _entries[i].Key;
                offsets[i] = currentOffset;
                int rowIdx = _groupHeads[i];
                int ptr = currentOffset;
                while (rowIdx != -1)
                {
                    indices[ptr++] = rowIdx;
                    rowIdx = RowNext[rowIdx];
                }
                currentOffset += _groupCounts[i];
            }
            offsets[_count] = currentOffset;
            return (keys, offsets, indices);
        }

        private static int GetPrime(int min)
        {
            int[] primes = { 101, 211, 431, 863, 1741, 3491, 6991, 13999, 28001, 56009, 112003, 224017, 448051, 896113, 1792241, 3584497, 7169003 };
            foreach (int p in primes) if (p >= min) return p;
            return min | 1;
        }

        public void Dispose()
        {
            _buckets = null!; _entries = null!; _groupHeads = null!; _groupTails = null!; _groupCounts = null!; RowNext = null!;
        }
    }
}