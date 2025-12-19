using System.Runtime.CompilerServices;

namespace LeichtFrame.Core.Engine
{
    internal static unsafe class PartitionedHistogram
    {
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public static void ComputeHistograms(
            int* pInput,
            int* pGlobalHistograms,
            int length,
            int min,
            int bucketCount,
            int numThreads)
        {
            int chunkSize = length / numThreads;

            Parallel.For(0, numThreads, t =>
            {
                int start = t * chunkSize;
                int end = (t == numThreads - 1) ? length : start + chunkSize;
                int* pLocalHist = pGlobalHistograms + (t * bucketCount);

                int i = start;

                // UNROLLING: 4x per Loop (reduces Branch Prediction Overhead)
                int endUnroll = end - 4;
                while (i < endUnroll)
                {
                    pLocalHist[pInput[i] - min]++;
                    pLocalHist[pInput[i + 1] - min]++;
                    pLocalHist[pInput[i + 2] - min]++;
                    pLocalHist[pInput[i + 3] - min]++;
                    i += 4;
                }

                // Tail
                while (i < end)
                {
                    pLocalHist[pInput[i] - min]++;
                    i++;
                }
            });
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public static void ScatterIndices(
            int* pInput,
            int* pFinalIndices, // Goal
            int* pWriteOffsets, // Cache
            int length,
            int min,
            int bucketCount,
            int numThreads)
        {
            int chunkSize = length / numThreads;

            Parallel.For(0, numThreads, t =>
            {
                int start = t * chunkSize;
                int end = (t == numThreads - 1) ? length : start + chunkSize;
                int* pLocalWriteOffsets = pWriteOffsets + (t * bucketCount);

                int i = start;

                int endUnroll = end - 4;
                while (i < endUnroll)
                {
                    // Item 1
                    int val1 = pInput[i];
                    int b1 = val1 - min;
                    int dest1 = pLocalWriteOffsets[b1];
                    pFinalIndices[dest1] = i;
                    pLocalWriteOffsets[b1]++;

                    // Item 2
                    int val2 = pInput[i + 1];
                    int b2 = val2 - min;
                    int dest2 = pLocalWriteOffsets[b2];
                    pFinalIndices[dest2] = i + 1;
                    pLocalWriteOffsets[b2]++;

                    // Item 3
                    int val3 = pInput[i + 2];
                    int b3 = val3 - min;
                    int dest3 = pLocalWriteOffsets[b3];
                    pFinalIndices[dest3] = i + 2;
                    pLocalWriteOffsets[b3]++;

                    // Item 4
                    int val4 = pInput[i + 3];
                    int b4 = val4 - min;
                    int dest4 = pLocalWriteOffsets[b4];
                    pFinalIndices[dest4] = i + 3;
                    pLocalWriteOffsets[b4]++;

                    i += 4;
                }

                while (i < end)
                {
                    int val = pInput[i];
                    int bucketIdx = val - min;
                    int destIdx = pLocalWriteOffsets[bucketIdx];
                    pFinalIndices[destIdx] = i;
                    pLocalWriteOffsets[bucketIdx]++;
                    i++;
                }
            });
        }
    }
}