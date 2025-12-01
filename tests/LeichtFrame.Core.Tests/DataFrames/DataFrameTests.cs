namespace LeichtFrame.Core.Tests.DataFrames
{
    public class DataFrameTests
    {
        [Fact]
        public void Constructor_Builds_Schema_And_Sets_Counts()
        {
            using var col1 = new IntColumn("Id", 10);
            col1.Append(1);

            using var col2 = new StringColumn("Name", 10);
            col2.Append("A");

            var df = new DataFrame(new IColumn[] { col1, col2 });

            Assert.Equal(1, df.RowCount);
            Assert.Equal(2, df.ColumnCount);

            Assert.True(df.Schema.HasColumn("Id"));
            Assert.True(df.Schema.HasColumn("Name"));
            Assert.Equal(typeof(int), df.Schema.Columns[0].DataType);
        }

        [Fact]
        public void Constructor_Throws_On_Length_Mismatch()
        {
            using var col1 = new IntColumn("Id", 10);
            col1.Append(1); // Length 1

            using var col2 = new StringColumn("Name", 10);
            col2.Append("A");
            col2.Append("B"); // Length 2

            var ex = Assert.Throws<ArgumentException>(() => new DataFrame(new IColumn[] { col1, col2 }));

            Assert.Contains("mismatch", ex.Message);
        }

        [Fact]
        public void Dispose_Calls_Dispose_On_Columns()
        {
            // Da wir schwer in die Columns "hineinschauen" können ob sie disposed sind (ohne Crash),
            // testen wir hier primär, dass df.Dispose() keine Fehler wirft.
            // Einen echten "wurde Dispose gerufen" Test bräuchte man Mocks (Moq), 
            // aber wir nutzen hier echte Klassen.

            var col = new IntColumn("Temp", 10);
            col.Append(1);

            var df = new DataFrame(new[] { col });

            df.Dispose();

            // Indirekter Beweis: Zugriff auf die Column sollte jetzt unsicher sein 
            // (oder bei IntColumn in A.2 Implementierung: _data ist null).
            Assert.ThrowsAny<Exception>(() => col.Get(0));
        }

        [Fact]
        public void Empty_DataFrame_Is_Valid()
        {
            var df = new DataFrame(new IColumn[0]);

            Assert.Equal(0, df.RowCount);
            Assert.Equal(0, df.ColumnCount);
            Assert.NotNull(df.Schema);
        }
    }
}