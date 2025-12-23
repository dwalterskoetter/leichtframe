using LeichtFrame.Core.Engine.Collections;

namespace LeichtFrame.Core.Tests.Engine
{
    public class NativeIntMapTests
    {
        [Fact]
        public void GetOrAdd_Assigns_Sequential_Ids()
        {
            using var map = new NativeIntMap(16);

            int id1 = map.GetOrAdd(100);
            int id2 = map.GetOrAdd(200);
            int id3 = map.GetOrAdd(300);

            Assert.Equal(0, id1);
            Assert.Equal(1, id2);
            Assert.Equal(2, id3);
            Assert.Equal(3, map.Count);
        }

        [Fact]
        public void GetOrAdd_Returns_Existing_Id_For_Duplicates()
        {
            using var map = new NativeIntMap(16);

            int idA = map.GetOrAdd(555);
            int idB = map.GetOrAdd(555);

            Assert.Equal(idA, idB);
            Assert.Equal(1, map.Count);
        }

        [Fact]
        public void Resize_Preserves_Data_And_Ids()
        {
            using var map = new NativeIntMap(16);
            int count = 1000;

            for (int i = 0; i < count; i++)
            {
                int id = map.GetOrAdd(i * 10);
                Assert.Equal(i, id);
            }

            Assert.Equal(count, map.Count);
            Assert.True(map.Capacity >= count);

            for (int i = 0; i < count; i++)
            {
                int id = map.GetOrAdd(i * 10);
                Assert.Equal(i, id);
            }
        }

        [Fact]
        public void ExportKeys_Returns_Correct_Array()
        {
            using var map = new NativeIntMap(16);
            map.GetOrAdd(10);
            map.GetOrAdd(20);
            map.GetOrAdd(30);

            int[] keys = map.ExportKeys();

            Assert.Equal(3, keys.Length);
            Assert.Equal(10, keys[0]);
            Assert.Equal(20, keys[1]);
            Assert.Equal(30, keys[2]);
        }

        [Fact]
        public void Collision_Handling_Works_With_Linear_Probing()
        {
            using var map = new NativeIntMap(128);
            var rnd = new Random(42);
            var dict = new Dictionary<int, int>();

            for (int i = 0; i < 1000; i++)
            {
                int key = rnd.Next();
                if (!dict.ContainsKey(key))
                {
                    int id = map.GetOrAdd(key);
                    dict[key] = id;
                }
                else
                {
                    int expectedId = dict[key];
                    int actualId = map.GetOrAdd(key);
                    Assert.Equal(expectedId, actualId);
                }
            }
        }
    }
}