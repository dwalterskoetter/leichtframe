namespace LeichtFrame.Core.Tests.Memory
{
    public class GrowthStrategyTests
    {
        [Fact]
        public void IntColumn_Grows_From_Small_To_Large_Without_DataLoss()
        {
            // Start extremely small (Capacity 2)
            using var col = new IntColumn("Growth", 2);
            int count = 10_000;

            // Loop Insert
            for (int i = 0; i < count; i++)
            {
                col.Append(i);
            }

            // Assert: Everything still there?
            Assert.Equal(count, col.Length);

            // Check samples
            Assert.Equal(0, col.Get(0));
            Assert.Equal(5000, col.Get(5000));
            Assert.Equal(9999, col.Get(9999));
        }

        [Fact]
        public void StringColumn_Grows_And_Preserves_Nulls()
        {
            // Start small
            using var col = new StringColumn("StrGrowth", 4, isNullable: true);
            int count = 1000;

            for (int i = 0; i < count; i++)
            {
                if (i % 2 == 0)
                    col.Append($"Item {i}");
                else
                    col.Append(null);
            }

            Assert.Equal(count, col.Length);

            // Verify Integrity after multiple resizes
            for (int i = 0; i < count; i++)
            {
                if (i % 2 == 0)
                {
                    Assert.False(col.IsNull(i));
                    Assert.Equal($"Item {i}", col.Get(i));
                }
                else
                {
                    Assert.True(col.IsNull(i));
                    Assert.Null(col.Get(i));
                }
            }
        }

        [Fact]
        public void Explicit_EnsureCapacity_Triggering()
        {
            using var col = new DoubleColumn("Explicit", 10);

            // Manually increase capacity
            col.EnsureCapacity(1000);

            // Check without exposing internal fields:
            // If we now add 1000 items, no further resize should be necessary 
            // (hard to test black-box, but we test that it doesn't crash and retains data)

            col.Append(3.14);
            Assert.Equal(3.14, col.Get(0));
        }

        [Fact]
        public void BoolColumn_BitPacking_Survives_Growth()
        {
            // BoolColumn is special (bit manipulation)
            using var col = new BoolColumn("Bits", 8); // 1 byte

            // Fill 1000 bits (crossing many byte boundaries)
            for (int i = 0; i < 1000; i++)
            {
                col.Append(true);
            }

            Assert.Equal(1000, col.Length);
            Assert.True(col.AllTrue()); // Must still be true
        }
    }
}