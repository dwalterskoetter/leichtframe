using LeichtFrame.Core;

namespace LeichtFrame.Core.Tests.DataFrames.Operations
{
    public class GroupByEdgeCasesTests
    {
        // ---------------------------------------------------------
        // 1. IntDirectMapStrategy: Negative Zahlen
        // ---------------------------------------------------------
        [Fact]
        public void GroupBy_NegativeIntegers_ShouldWorkWithDirectMap()
        {
            // Range: -100 bis -10 (Differenz 90) -> Triggert IntDirectMapStrategy
            var df = DataFrame.FromObjects(new[]
            {
                new { Temp = -10 },
                new { Temp = -50 },
                new { Temp = -100 },
                new { Temp = -10 }, // Duplikat
                new { Temp = -100 } // Duplikat
            });

            var result = df.GroupBy("Temp").Count();

            Assert.Equal(3, result.RowCount); // Gruppen: -10, -50, -100

            // Check Counts
            var rowMinus10 = result.Where(r => r.Get<int>("Temp") == -10);
            Assert.Equal(2, rowMinus10["Count"].Get<int>(0));

            var rowMinus100 = result.Where(r => r.Get<int>("Temp") == -100);
            Assert.Equal(2, rowMinus100["Count"].Get<int>(0));
        }

        // ---------------------------------------------------------
        // 2. Dispatcher / IntRadixStrategy: Sparse Data
        // ---------------------------------------------------------
        [Fact]
        public void GroupBy_LargeRange_SparseData_ShouldNotCrash()
        {
            // Range = 2.000.000 -> Triggert IntRadixStrategy (da > 1M)
            var df = DataFrame.FromObjects(new[]
            {
                new { Id = 0 },
                new { Id = 2_000_000 }
            });

            var result = df.GroupBy("Id").Count();

            Assert.Equal(2, result.RowCount);

            var rowLarge = result.Where(r => r.Get<int>("Id") == 2_000_000);
            Assert.Equal(1, rowLarge["Count"].Get<int>(0));
        }

        // ---------------------------------------------------------
        // 3. GenericHashMapStrategy: DateTime
        // ---------------------------------------------------------
        [Fact]
        public void GroupBy_DateTime_ShouldGroupCorrectly()
        {
            var date1 = new DateTime(2023, 1, 1);
            var date2 = new DateTime(2023, 1, 2);

            var df = DataFrame.FromObjects(new[]
            {
                new { Date = date1 },
                new { Date = date2 },
                new { Date = date1 }
            });

            var result = df.GroupBy("Date").Count();

            Assert.Equal(2, result.RowCount);

            var d1Row = result.Where(r => r.Get<DateTime>("Date") == date1);
            Assert.Equal(2, d1Row["Count"].Get<int>(0));
        }

        // ---------------------------------------------------------
        // 4. GenericHashMapStrategy: Bool & Nulls
        // ---------------------------------------------------------
        [Fact]
        public void GroupBy_Bool_WithNulls_ShouldGroupCorrectly()
        {
            // KORREKTUR HIER: Expliziter Cast auf (bool?), damit alle Elemente denselben Typ haben.
            var df = DataFrame.FromObjects(new[]
            {
                new { Active = (bool?)true },
                new { Active = (bool?)false },
                new { Active = (bool?)true },
                new { Active = (bool?)null }
            });

            // Sollte GenericHashMapStrategy nutzen
            var result = df.GroupBy("Active").Count();

            // Erwartung: True(2), False(1), Null(1)
            Assert.Equal(3, result.RowCount);

            // Prüfen ob True korrekt gezählt wurde
            // Achtung: Wenn Nullable Bool gefiltert wird, müssen wir sicherstellen, dass r.Get<bool?> genutzt wird oder IsNull checken.
            var trueRow = result.Where(r => !r.IsNull("Active") && r.Get<bool>("Active") == true);

            Assert.Equal(2, trueRow["Count"].Get<int>(0));

            // Prüfen ob Null-Gruppe existiert
            bool hasNullGroup = false;
            for (int i = 0; i < result.RowCount; i++)
            {
                if (result["Active"].IsNull(i))
                {
                    hasNullGroup = true;
                    Assert.Equal(1, result["Count"].Get<int>(i));
                }
            }
            Assert.True(hasNullGroup, "Null group for Bool column missing");
        }
    }
}