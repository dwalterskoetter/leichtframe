using LeichtFrame.Core.Engine.Collections;
using LeichtFrame.Core.Engine.Kernels.GroupBy.Strategies;
using FluentAssertions;

namespace LeichtFrame.Core.Tests.Engine.Collections
{
    public class NativeStringMapTests
    {
        [Fact]
        public unsafe void GetOrAdd_Inserts_And_Finds_Strings()
        {
            string s = "AlphaBetaAlpha";
            byte[] bytes = System.Text.Encoding.UTF8.GetBytes(s);
            int[] offsets = { 0, 5, 9, 14 };

            fixed (byte* pBytes = bytes)
            fixed (int* pOffsets = offsets)
            {
                using var map = new NativeStringMap(16, pBytes, pOffsets);

                int h1 = 100;
                int h2 = 200;

                int g1 = map.GetOrAdd(0, h1);
                int g2 = map.GetOrAdd(1, h2);
                int g3 = map.GetOrAdd(2, h1);

                Assert.Equal(0, g1);
                Assert.Equal(1, g2);
                Assert.Equal(0, g3);
                Assert.Equal(2, map.Count);
            }
        }

        [Fact]
        public unsafe void GermanString_Prefix_Optimization_Works_With_Short_Strings()
        {
            string s = "HiHoHi";
            byte[] bytes = System.Text.Encoding.UTF8.GetBytes(s);
            int[] offsets = { 0, 2, 4, 6 };

            fixed (byte* pBytes = bytes)
            fixed (int* pOffsets = offsets)
            {
                using var map = new NativeStringMap(16, pBytes, pOffsets);

                int h1 = 10;
                int h2 = 20;

                int id1 = map.GetOrAdd(0, h1);
                int id2 = map.GetOrAdd(1, h2);
                int id3 = map.GetOrAdd(2, h1);

                Assert.Equal(0, id1);
                Assert.Equal(1, id2);
                Assert.Equal(0, id3);
            }
        }

        [Fact]
        public unsafe void Resize_Works_Correctly()
        {
            int count = 100;
            string s = string.Join("", Enumerable.Range(0, count).Select(i => i.ToString() + ","));
            byte[] bytes = System.Text.Encoding.UTF8.GetBytes(s);

            var offsetsList = new List<int> { 0 };
            for (int i = 0; i < bytes.Length; i++)
            {
                if (bytes[i] == ',') offsetsList.Add(i + 1);
            }
            int[] offsets = offsetsList.ToArray();

            fixed (byte* pBytes = bytes)
            fixed (int* pOffsets = offsets)
            {
                using var map = new NativeStringMap(16, pBytes, pOffsets);

                for (int i = 0; i < count; i++)
                {
                    map.GetOrAdd(i, i * 12345);
                }

                Assert.Equal(count, map.Count);
                Assert.True(map.Capacity >= count);
            }
        }

        [Fact]
        public void Group_HighCardUUIDs_ShouldTriggerAVX2Path()
        {
            var col = new StringColumn("UUID", 1000);

            string baseUuid = "12345678-1234-1234-1234-123456789012";

            col.Append(baseUuid);
            col.Append(baseUuid);

            col.Append("9" + baseUuid.Substring(1));

            col.Append(baseUuid.Substring(0, 35) + "X");

            char[] chars = baseUuid.ToCharArray();
            chars[31] = 'X';
            col.Append(new string(chars));

            var df = new DataFrame(new IColumn[] { col });
            var strategy = new StringSwissMapStrategy();

            using var gdf = strategy.Group(df, "UUID");

            gdf.GroupCount.Should().Be(4);

            var counts = gdf.Count().Columns[1] as IntColumn;
            counts!.Values.ToArray().Should().Contain(2);
        }
    }
}