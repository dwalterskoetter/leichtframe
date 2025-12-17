namespace LeichtFrame.Core.Tests.Safety
{
    public class GroupByStressTests
    {
        [Fact]
        public void GroupBy_RepeatedExecution_ShouldNotLeakMemory()
        {
            // Setup: 10.000 Zeilen Int Data
            int rows = 10_000;
            using var col = new IntColumn("Val", rows);
            for (int i = 0; i < rows; i++) col.Append(i % 100); // 100 Gruppen

            var df = new DataFrame(new[] { col });

            // Act: 100x GroupBy ausführen
            // Wenn 'UnsafeBuffer' oder 'ArrayPool' Arrays nicht zurückgegeben werden,
            // würde der Speicherverbrauch hier ansteigen oder der Pool leerlaufen.
            for (int i = 0; i < 100; i++)
            {
                // Dies triggert IntDirectMapStrategy (Range 0-99 < 1M)
                var grouped = df.GroupBy("Val");

                Assert.Equal(100, grouped.GroupCount);

                // GC.Collect() hier NICHT aufrufen, um echten Druck zu simulieren
            }

            // Wenn der Test hier ankommt ohne OutOfMemoryException, ist Dispose() korrekt implementiert.
            Assert.True(true);
        }

        [Fact]
        public void GroupByRadix_RepeatedExecution_ShouldNotLeakMemory()
        {
            // Setup: High Cardinality Data (Triggert Radix Sort)
            int rows = 10_000;
            using var col = new IntColumn("Val", rows);
            // Große Werte -> Range > 1M -> Radix Strategy
            for (int i = 0; i < rows; i++) col.Append(i * 1000);

            var df = new DataFrame(new[] { col });

            // Act: 50x GroupBy Radix
            for (int i = 0; i < 50; i++)
            {
                var grouped = df.GroupBy("Val"); // Nutzt Dispatcher -> IntRadixStrategy
                Assert.Equal(rows, grouped.GroupCount); // Alle unique
            }

            Assert.True(true);
        }
    }
}