using System.Runtime.InteropServices;

namespace LeichtFrame.Core.Engine.Algorithms.Partitioning
{
    internal static unsafe class RadixPartitioner
    {
        public static void Partition(
            int* pHashes,
            int rowCount,
            int partitionCount,
            int shift,
            out int* pOutHashes,
            out int* pOutRowIndices,
            int[] partitionOffsets)
        {
            // Alloc Output
            pOutHashes = (int*)NativeMemory.Alloc((nuint)(rowCount * sizeof(int)));
            pOutRowIndices = (int*)NativeMemory.Alloc((nuint)(rowCount * sizeof(int)));

            int numThreads = Environment.ProcessorCount;
            int chunkSize = (rowCount + numThreads - 1) / numThreads;

            // --- STEP 1: Thread-Local Histograms ---
            int* histograms = (int*)NativeMemory.AllocZeroed((nuint)(numThreads * partitionCount * sizeof(int)));

            int* localPtrHashes = pHashes;

            Parallel.For(0, numThreads, t =>
            {
                int start = t * chunkSize;
                int end = Math.Min(start + chunkSize, rowCount);
                int* localHist = histograms + (t * partitionCount);

                for (int i = start; i < end; i++)
                {
                    int p = (int)((uint)localPtrHashes[i] >> shift);
                    localHist[p]++;
                }
            });

            // --- STEP 2: Prefix Sum ---
            int* writeOffsets = (int*)NativeMemory.Alloc((nuint)(numThreads * partitionCount * sizeof(int)));
            int currentGlobal = 0;

            for (int p = 0; p < partitionCount; p++)
            {
                partitionOffsets[p] = currentGlobal;
                for (int t = 0; t < numThreads; t++)
                {
                    int count = histograms[t * partitionCount + p];
                    writeOffsets[t * partitionCount + p] = currentGlobal;
                    currentGlobal += count;
                }
            }
            partitionOffsets[partitionCount] = currentGlobal;

            // --- STEP 3: Scatter ---
            int* localPtrOutHashes = pOutHashes;
            int* localPtrOutIndices = pOutRowIndices;

            Parallel.For(0, numThreads, t =>
            {
                int start = t * chunkSize;
                int end = Math.Min(start + chunkSize, rowCount);
                int* localOffsets = writeOffsets + (t * partitionCount);

                for (int i = start; i < end; i++)
                {
                    int hash = localPtrHashes[i];
                    int p = (int)((uint)hash >> shift);

                    int dest = localOffsets[p];

                    localPtrOutHashes[dest] = hash;
                    localPtrOutIndices[dest] = i;

                    localOffsets[p]++;
                }
            });

            NativeMemory.Free(histograms);
            NativeMemory.Free(writeOffsets);
        }
    }
}