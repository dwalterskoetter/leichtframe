using LeichtFrame.Core.Engine;

namespace LeichtFrame.Core.Logic
{
    /// <summary>
    /// Hält das Ergebnis eines GroupBy im unmanaged Memory.
    /// Wird verwendet, um Allocations zu vermeiden, bis der User die Daten wirklich braucht.
    /// </summary>
    internal unsafe class NativeGroupedData : IDisposable
    {
        public UnsafeBuffer<int> Keys;      // Die Gruppen-Keys
        public UnsafeBuffer<int> Offsets;   // CSR Offsets
        public UnsafeBuffer<int> Indices;   // CSR Indices (Sorted row pointers)
        public int GroupCount;
        public int RowCount;

        public bool IsDisposed { get; private set; }

        public NativeGroupedData(int rowCount, int groupCount)
        {
            RowCount = rowCount;
            GroupCount = groupCount;

            // Wir allokieren im Native Heap -> GC sieht das nicht!
            Indices = new UnsafeBuffer<int>(rowCount);
            Offsets = new UnsafeBuffer<int>(groupCount + 1);
            Keys = new UnsafeBuffer<int>(groupCount);
        }

        public void Dispose()
        {
            if (!IsDisposed)
            {
                Keys.Dispose();
                Offsets.Dispose();
                Indices.Dispose();
                IsDisposed = true;
            }
        }
    }
}