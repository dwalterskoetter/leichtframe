using LeichtFrame.Core.Engine;

namespace LeichtFrame.Core.Tests.Engine
{
    public class PartitionedHistogramTests
    {
        [Fact]
        public unsafe void ComputeHistograms_Counts_Correctly_With_Multiple_Threads()
        {
            // Arrange
            // Daten: [10, 11, 10, 12]
            // Min: 10, Max: 12 -> Range: 2 -> BucketCount: 3 (0, 1, 2)
            int[] input = { 10, 11, 10, 12 };
            int length = input.Length;
            int min = 10;
            int bucketCount = 3;
            int numThreads = 2; // Wir erzwingen 2 Threads

            // Histogramm Größe: Threads * Buckets = 2 * 3 = 6
            int[] histograms = new int[numThreads * bucketCount];

            // Act
            fixed (int* pInput = input)
            fixed (int* pHist = histograms)
            {
                PartitionedHistogram.ComputeHistograms(
                    pInput,
                    pHist,
                    length,
                    min,
                    bucketCount,
                    numThreads
                );
            }

            // Assert
            // Thread 0 bearbeitet indices 0, 1 -> Werte [10, 11]
            // Thread 1 bearbeitet indices 2, 3 -> Werte [10, 12]

            // Layout: [Thread0_B0, Thread0_B1, Thread0_B2, Thread1_B0, Thread1_B1, Thread1_B2]

            // Thread 0:
            Assert.Equal(1, histograms[0]); // 1x Wert 10
            Assert.Equal(1, histograms[1]); // 1x Wert 11
            Assert.Equal(0, histograms[2]); // 0x Wert 12

            // Thread 1:
            Assert.Equal(1, histograms[3]); // 1x Wert 10
            Assert.Equal(0, histograms[4]); // 0x Wert 11
            Assert.Equal(1, histograms[5]); // 1x Wert 12
        }

        [Fact]
        public unsafe void ScatterIndices_Writes_To_Correct_Positions()
        {
            // Arrange
            // Gleiche Daten: [10, 11, 10, 12]
            int[] input = { 10, 11, 10, 12 };
            int length = input.Length;
            int min = 10;
            int bucketCount = 3;
            int numThreads = 2;

            // Wir simulieren das Ergebnis der Prefix-Summe manuell.
            // Wo sollen die Threads schreiben?
            // Ziel-Array Index-Verteilung:
            // Bucket 0 (Wert 10): 2 items. StartIdx 0.
            // Bucket 1 (Wert 11): 1 item. StartIdx 2.
            // Bucket 2 (Wert 12): 1 item. StartIdx 3.

            // Thread 0 hat: 1x 10, 1x 11.
            // Thread 1 hat: 1x 10, 1x 12.

            // WriteOffsets Array (Größe: Threads * Buckets = 6):
            int[] writeOffsets = new int[6];

            // T0, B0 (10): Startet bei 0
            writeOffsets[0] = 0;
            // T0, B1 (11): Startet bei 2 (nach den zwei 10ern)
            writeOffsets[1] = 2;
            // T0, B2 (12): Irrelevant (hat keine), aber wäre bei 3
            writeOffsets[2] = 3;

            // T1, B0 (10): Startet bei 1 (nachdem T0 seinen einen 10er geschrieben hat)
            writeOffsets[3] = 1;
            // T1, B1 (11): Irrelevant
            writeOffsets[4] = 3; // Nach der 11 von T0
            // T1, B2 (12): Startet bei 3 (nach der 11)
            writeOffsets[5] = 3;

            int[] finalIndices = new int[4];

            // Act
            fixed (int* pInput = input)
            fixed (int* pFinal = finalIndices)
            fixed (int* pOffsets = writeOffsets)
            {
                PartitionedHistogram.ScatterIndices(
                    pInput,
                    pFinal,
                    pOffsets,
                    length,
                    min,
                    bucketCount,
                    numThreads
                );
            }

            // Assert
            // Erwartung im finalIndices:
            // Pos 0: Index einer 10 (von Thread 0 -> Index 0)
            // Pos 1: Index einer 10 (von Thread 1 -> Index 2)
            // Pos 2: Index einer 11 (von Thread 0 -> Index 1)
            // Pos 3: Index einer 12 (von Thread 1 -> Index 3)

            Assert.Equal(0, finalIndices[0]); // Wert 10
            Assert.Equal(2, finalIndices[1]); // Wert 10
            Assert.Equal(1, finalIndices[2]); // Wert 11
            Assert.Equal(3, finalIndices[3]); // Wert 12
        }

        [Fact]
        public unsafe void Integration_ComputeAndScatter_ShouldSortIndicesByValue()
        {
            int[] input = { 50, 20, 50, 10, 20, 50 }; // Indizes: 0,1,2,3,4,5
            int min = 10;

            // KORREKTUR: Wir nutzen max um range zu berechnen, oder lassen max weg.
            // Hier entfernen wir 'max' einfach, da wir 'range' direkt definieren.
            int range = 40;

            int bucketCount = range + 1;
            int numThreads = 2; // T0: 0-2 [50, 20, 50], T1: 3-5 [10, 20, 50]

            int[] histograms = new int[numThreads * bucketCount];
            int[] offsets = new int[numThreads * bucketCount];
            int[] resultIndices = new int[input.Length];

            fixed (int* pIn = input)
            fixed (int* pHist = histograms)
            fixed (int* pOff = offsets)
            fixed (int* pRes = resultIndices)
            {
                // 1. Count
                PartitionedHistogram.ComputeHistograms(pIn, pHist, input.Length, min, bucketCount, numThreads);

                // 2. Prefix Sum (Simulierte Logic-Schicht)
                int globalPos = 0;
                for (int b = 0; b < bucketCount; b++)
                {
                    for (int t = 0; t < numThreads; t++)
                    {
                        int count = pHist[t * bucketCount + b];
                        if (count > 0)
                        {
                            pOff[t * bucketCount + b] = globalPos;
                            globalPos += count;
                        }
                    }
                }

                // 3. Scatter
                PartitionedHistogram.ScatterIndices(pIn, pRes, pOff, input.Length, min, bucketCount, numThreads);
            }

            // Assert Result
            Assert.Equal(10, input[resultIndices[0]]);
            Assert.Equal(20, input[resultIndices[1]]);
            Assert.Equal(20, input[resultIndices[2]]);
            Assert.Equal(50, input[resultIndices[3]]);
            Assert.Equal(50, input[resultIndices[4]]);
            Assert.Equal(50, input[resultIndices[5]]);
        }
    }
}