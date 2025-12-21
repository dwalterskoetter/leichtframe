using LeichtFrame.Core;

namespace LeichtFrame.Core.Tests.Execution
{
    public class MoveSemanticsTests
    {
        [Fact]
        public void DoubleColumn_Adopt_Constructor_Does_Not_Copy()
        {
            // Arrange
            int length = 5;
            double[] rawData = new double[] { 1.0, 2.0, 3.0, 4.0, 5.0 };

            // Act: Rufe den internen Move-Konstruktor auf
            // (Möglich dank InternalsVisibleTo)
            var col = new DoubleColumn("Moved", rawData, length);

            // Assert 1: Werte stimmen
            Assert.Equal(1.0, col.Get(0));

            // Assert 2: Proof of Shared Memory (Zero Copy)
            // Wir ändern das Array "von außen". Wenn col eine Kopie hätte, würde sich nichts ändern.
            rawData[0] = 999.9;

            Assert.Equal(999.9, col.Get(0));
        }

        [Fact]
        public void IntColumn_Adopt_Constructor_Does_Not_Copy()
        {
            // Arrange
            int length = 3;
            int[] rawData = new int[] { 10, 20, 30 };

            // Act
            var col = new IntColumn("MovedInt", rawData, length);

            // Assert Proof of Shared Memory
            rawData[1] = 555;

            Assert.Equal(555, col.Get(1));
        }
    }
}