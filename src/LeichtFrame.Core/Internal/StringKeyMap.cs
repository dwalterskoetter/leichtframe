using System.Runtime.CompilerServices;
using System.Text;

namespace LeichtFrame.Core.Internal
{
    internal sealed class StringKeyMap : IDisposable
    {
        private int[] _buckets;
        private Entry[] _entries;
        private int _count;
        private int _freeList;

        private readonly byte[] _bytes;
        private readonly int[] _offsets;

        private int[] _groupHeads;
        private int[] _groupTails;
        private int[] _groupCounts;

        public int[] RowNext;

        private struct Entry
        {
            public int HashCode;
            public int Next;
            public int FirstGlobalRowIdx;
        }

        public int Count => _count;

        // capacity: Geschätzte Anzahl Gruppen
        // localRowCount: Größe des lokalen RowNext Arrays (Partition Size)
        public StringKeyMap(byte[] bytes, int[] offsets, int capacity, int localRowCount)
        {
            _bytes = bytes;
            _offsets = offsets;

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

            RowNext = new int[localRowCount];
            Array.Fill(RowNext, -1);
        }

        // Sequential: Global Index == Local Index
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AddRow(int globalRowIndex)
        {
            AddRow(globalRowIndex, globalRowIndex);
        }

        // Parallel: Global Index (für Datenzugriff) != Local Index (für LinkedList Speicher)
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AddRow(int globalRowIndex, int localRowIndex)
        {
            // 1. Lookup (Hash Global Data)
            int start = _offsets[globalRowIndex];
            int len = _offsets[globalRowIndex + 1] - start;
            int hashCode = ComputeHash(start, len);
            int bucket = (hashCode & 0x7FFFFFFF) % _buckets.Length;

            int groupIndex = -1;
            for (int i = _buckets[bucket]; i >= 0; i = _entries[i].Next)
            {
                if (_entries[i].HashCode == hashCode)
                {
                    int repRow = _entries[i].FirstGlobalRowIdx;
                    int repStart = _offsets[repRow];
                    int repLen = _offsets[repRow + 1] - repStart;
                    if (len == repLen)
                    {
                        var span = new ReadOnlySpan<byte>(_bytes, start, len);
                        var repSpan = new ReadOnlySpan<byte>(_bytes, repStart, repLen);
                        if (span.SequenceEqual(repSpan))
                        {
                            groupIndex = i;
                            break;
                        }
                    }
                }
            }

            // 2. Insert new Group
            if (groupIndex == -1)
            {
                if (_freeList >= 0) { groupIndex = _freeList; _freeList = _entries[groupIndex].Next; }
                else
                {
                    if (_count == _entries.Length) { Resize(); bucket = (hashCode & 0x7FFFFFFF) % _buckets.Length; }
                    groupIndex = _count;
                    _count++;
                }

                ref Entry entry = ref _entries[groupIndex];
                entry.HashCode = hashCode;
                entry.Next = _buckets[bucket];
                entry.FirstGlobalRowIdx = globalRowIndex;
                _buckets[bucket] = groupIndex;
            }

            // 3. Append to Linked List (using Local Index)
            int tail = _groupTails[groupIndex];
            if (tail == -1)
            {
                _groupHeads[groupIndex] = localRowIndex;
            }
            else
            {
                RowNext[tail] = localRowIndex;
            }
            _groupTails[groupIndex] = localRowIndex;
            RowNext[localRowIndex] = -1;

            _groupCounts[groupIndex]++;
        }

        public (string[] Keys, int[] Offsets, int[] Indices) ToCSR()
        {
            var keys = new string[_count];
            var offsets = new int[_count + 1];

            int totalRows = 0;
            for (int i = 0; i < _count; i++) totalRows += _groupCounts[i];

            var indices = new int[totalRows];
            int currentOffset = 0;

            for (int i = 0; i < _count; i++)
            {
                // Materialize Key
                int repRow = _entries[i].FirstGlobalRowIdx;
                int start = _offsets[repRow];
                int len = _offsets[repRow + 1] - start;
                keys[i] = Encoding.UTF8.GetString(_bytes, start, len);

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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int ComputeHash(int start, int length)
        {
            int hash = -2128831035;
            int end = start + length;
            for (int k = start; k < end; k++)
                hash = (hash ^ _bytes[k]) * 16777619;
            return hash;
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
                int bucket = (newEntries[i].HashCode & 0x7FFFFFFF) % newSize;
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