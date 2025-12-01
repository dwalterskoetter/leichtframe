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

        [Fact]
        public void Indexer_By_Int_Returns_Correct_Column()
        {
            using var col1 = new IntColumn("Col1", 5);
            using var col2 = new IntColumn("Col2", 5);
            var df = new DataFrame(new[] { col1, col2 });

            Assert.Same(col1, df[0]);
            Assert.Same(col2, df[1]);
        }

        [Fact]
        public void Indexer_By_Int_Throws_On_Invalid_Index()
        {
            var df = new DataFrame(new IColumn[0]);
            Assert.Throws<ArgumentOutOfRangeException>(() => df[0]);
        }

        [Fact]
        public void Indexer_By_Name_Returns_Correct_Column()
        {
            using var age = new IntColumn("Age", 5);
            using var name = new StringColumn("Name", 5);
            var df = new DataFrame(new IColumn[] { age, name });

            Assert.Same(age, df["Age"]);
            Assert.Same(name, df["Name"]);
        }

        [Fact]
        public void Indexer_By_Name_Throws_If_Missing()
        {
            using var col = new IntColumn("Data", 5);
            var df = new DataFrame(new[] { col });

            // Exception comes from Schema.GetColumnIndex
            Assert.Throws<ArgumentException>(() => df["Missing"]);
        }

        [Fact]
        public void TryGetColumn_Returns_False_If_Missing()
        {
            using var col = new IntColumn("Data", 5);
            var df = new DataFrame(new[] { col });

            bool found = df.TryGetColumn("Missing", out var result);

            Assert.False(found);
            Assert.Null(result);
        }

        [Fact]
        public void TryGetColumn_Returns_True_And_Column_If_Found()
        {
            using var col = new IntColumn("Data", 5);
            var df = new DataFrame(new[] { col });

            bool found = df.TryGetColumn("Data", out var result);

            Assert.True(found);
            Assert.Same(col, result);
        }

        [Fact]
        public void Create_Factory_Builds_Correct_Structure_From_Schema()
        {
            // 1. Define Schema (Blueprint)
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Id", typeof(int), IsNullable: false),
                new ColumnDefinition("Value", typeof(double), IsNullable: true),
                new ColumnDefinition("Label", typeof(string))
            });

            // 2. Create via Factory
            var df = DataFrame.Create(schema, capacity: 100);

            // 3. Verify Basics
            Assert.Equal(0, df.RowCount); // Muss leer sein
            Assert.Equal(3, df.ColumnCount);

            // 4. Verify Columns match Schema
            // Check ID
            var idCol = df["Id"];
            Assert.IsType<IntColumn>(idCol);
            Assert.False(idCol.IsNullable);

            // Check Value
            var valCol = df["Value"];
            Assert.IsType<DoubleColumn>(valCol);
            Assert.True(valCol.IsNullable);

            // 5. Verify Capacity (indirectly via functionality)
            ((IntColumn)idCol).Append(1);
            ((DoubleColumn)valCol).Append(null);
            ((StringColumn)df["Label"]).Append("Test");

            Assert.Equal(1, df.RowCount);
        }
    }
}