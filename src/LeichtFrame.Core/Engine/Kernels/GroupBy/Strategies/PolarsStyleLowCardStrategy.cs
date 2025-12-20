using System.Runtime.InteropServices;

namespace LeichtFrame.Core.Engine
{
    internal unsafe class PolarsStyleLowCardStrategy : IGroupByStrategy
    {
        private readonly int? _knownMin;
        private readonly int? _knownMax;

        public PolarsStyleLowCardStrategy(int? min = null, int? max = null)
        {
            _knownMin = min;
            _knownMax = max;
        }

        public GroupedDataFrame Group(DataFrame df, string columnName)
        {
            var col = (IntColumn)df[columnName];

            // 1. Alles Nativ berechnen
            NativeGroupedData nativeData = ComputeNative(col, df.RowCount);

            // 2. KEIN COPY! KEIN DISPOSE!
            // Wir übergeben den Besitz (Ownership) an den DataFrame.
            // Der DataFrame ruft Dispose() auf nativeData auf, wenn er selbst disposed wird (nach Aggregation).

            return new GroupedDataFrame<int>(df, new[] { columnName }, nativeData);
        }

        internal NativeGroupedData ComputeNative(IntColumn col, int rowCount)
        {
            // ... (Dieser Teil bleibt exakt gleich wie in deinem Code) ...
            // ... ComputeHistograms ... PrefixSum ... Scatter ...

            // WICHTIG: Kein finally { Dispose } für das result Object hier!
            // Das Resultat muss leben bleiben.

            // Hier der gekürzte Body, da er unverändert ist:
            if (rowCount == 0) return new NativeGroupedData(0, 0);

            int min = _knownMin ?? col.Min();
            int max = _knownMax ?? col.Max();
            long range = (long)max - min;
            int bucketCount = (int)range + 1;
            int numThreads = Environment.ProcessorCount;

            nuint histogramSize = (nuint)(numThreads * bucketCount * sizeof(int));

            int* pGlobalHistograms = (int*)NativeMemory.AllocZeroed(histogramSize);
            int* pWriteOffsets = (int*)NativeMemory.Alloc(histogramSize);

            var result = new NativeGroupedData(rowCount, bucketCount);

            try
            {
                fixed (int* pInput = col.Values.Span)
                {
                    PartitionedHistogram.ComputeHistograms(pInput, pGlobalHistograms, rowCount, min, bucketCount, numThreads);

                    int* pOffsets = result.Offsets.Ptr;
                    int* pKeys = result.Keys.Ptr;
                    pOffsets[0] = 0;
                    int currentGlobalOffset = 0;
                    int activeGroups = 0;

                    for (int b = 0; b < bucketCount; b++)
                    {
                        int globalCountForBucket = 0;
                        for (int t = 0; t < numThreads; t++)
                        {
                            int localCount = pGlobalHistograms[t * bucketCount + b];
                            if (localCount > 0)
                            {
                                pWriteOffsets[t * bucketCount + b] = currentGlobalOffset + globalCountForBucket;
                                globalCountForBucket += localCount;
                            }
                        }

                        if (globalCountForBucket > 0)
                        {
                            pKeys[activeGroups] = b + min;
                            currentGlobalOffset += globalCountForBucket;
                            activeGroups++;
                            pOffsets[activeGroups] = currentGlobalOffset;
                        }
                    }

                    result.GroupCount = activeGroups;

                    PartitionedHistogram.ScatterIndices(pInput, result.Indices.Ptr, pWriteOffsets, rowCount, min, bucketCount, numThreads);
                }
                return result;
            }
            finally
            {
                // Nur die temporären Buffer aufräumen!
                NativeMemory.Free(pGlobalHistograms);
                NativeMemory.Free(pWriteOffsets);
            }
        }
    }
}