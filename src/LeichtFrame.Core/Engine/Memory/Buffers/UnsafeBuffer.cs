using System.Runtime.InteropServices;

namespace LeichtFrame.Core.Engine.Memory
{
    /// <summary>
    /// Wrapper around NativeMemory to avoid GC pressure for large internal buffers.
    /// </summary>
    internal unsafe struct UnsafeBuffer<T> : IDisposable where T : unmanaged
    {
        public T* Ptr;
        public int Length;

        public UnsafeBuffer(int length)
        {
            Length = length;
            Ptr = (T*)NativeMemory.Alloc((nuint)length, (nuint)sizeof(T));
        }

        public void Dispose()
        {
            if (Ptr != null)
            {
                NativeMemory.Free(Ptr);
                Ptr = null;
            }
        }

        public ref T this[int index] => ref Ptr[index];
    }
}