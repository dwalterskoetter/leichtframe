using System.Runtime.InteropServices;
using LeichtFrame.Core.Engine.Algorithms.Partitioning;

namespace LeichtFrame.Core.Tests.Engine.Algorithms.Partitioning
{
    public class RadixPartitionerTests
    {
        [Fact]
        public unsafe void Partition_Shuffles_Hashes_Into_Correct_Buckets()
        {
            int count = 4;
            int partitionCount = 2;
            int shift = 31;

            int* inputHashes = (int*)NativeMemory.Alloc((nuint)(count * sizeof(int)));
            inputHashes[0] = 0;
            inputHashes[1] = -1;
            inputHashes[2] = int.MaxValue;
            inputHashes[3] = int.MinValue;

            int* outHashes = null;
            int* outIndices = null;

            int[] offsets = new int[partitionCount + 1];

            try
            {
                RadixPartitioner.Partition(inputHashes, count, partitionCount, shift, out outHashes, out outIndices, offsets);

                Assert.Equal(0, offsets[0]);
                Assert.Equal(2, offsets[1]);
                Assert.Equal(4, offsets[2]);

                var p0_Hashes = new[] { outHashes[0], outHashes[1] };
                Assert.Contains(0, p0_Hashes);
                Assert.Contains(int.MaxValue, p0_Hashes);

                var p1_Hashes = new[] { outHashes[2], outHashes[3] };
                Assert.Contains(-1, p1_Hashes);
                Assert.Contains(int.MinValue, p1_Hashes);

                for (int i = 0; i < 4; i++)
                {
                    if (outHashes[i] == 0) Assert.Equal(0, outIndices[i]);
                    if (outHashes[i] == -1) Assert.Equal(1, outIndices[i]);
                }
            }
            finally
            {
                NativeMemory.Free(inputHashes);
                if (outHashes != null) NativeMemory.Free(outHashes);
                if (outIndices != null) NativeMemory.Free(outIndices);
            }
        }
    }
}