using LeichtFrame.Core;
using LeichtFrame.Core.Expressions;
using static LeichtFrame.Core.Expressions.F;

namespace LeichtFrame.Core.Tests.Lazy
{
    public class MultiColumnGroupByTests
    {
        [Fact]
        public void GroupBy_TwoColumns_String_Int_SumsCorrectly()
        {
            var df = DataFrame.FromObjects(new[]
            {
                new { Dept = "IT", Year = 2023, Amount = 100 },
                new { Dept = "IT", Year = 2023, Amount = 50 },
                new { Dept = "IT", Year = 2024, Amount = 200 },
                new { Dept = "HR", Year = 2023, Amount = 300 },
                new { Dept = "HR", Year = 2023, Amount = 100 }
            });

            // FIX: Direkt Aggregate aufrufen, nicht vorher GroupBy
            var result = df.Lazy()
                           .Aggregate(
                                new[] { Col("Dept"), Col("Year") },
                                new[] { Sum(Col("Amount")).As("Total") }
                           )
                           .Collect();

            Assert.Equal(3, result.RowCount);

            var it23 = result.Where(r => r.Get<string>("Dept") == "IT" && r.Get<int>("Year") == 2023);
            Assert.Equal(150.0, it23["Total"].Get<double>(0));

            var hr23 = result.Where(r => r.Get<string>("Dept") == "HR" && r.Get<int>("Year") == 2023);
            Assert.Equal(400.0, hr23["Total"].Get<double>(0));
        }

        [Fact]
        public void GroupBy_MultipleColumns_With_Nulls_HandlesNullsAsValue()
        {
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Cat", typeof(string)),
                new ColumnDefinition("SubCat", typeof(int), IsNullable: true),
                new ColumnDefinition("Val", typeof(int))
            });

            var df = DataFrame.Create(schema, 4);
            var cat = (StringColumn)df["Cat"];
            var sub = (IntColumn)df["SubCat"];
            var val = (IntColumn)df["Val"];

            cat.Append("A"); sub.Append(null); val.Append(10);
            cat.Append("A"); sub.Append(null); val.Append(20);
            cat.Append("A"); sub.Append(1); val.Append(5);
            cat.Append("B"); sub.Append(null); val.Append(100);

            // FIX: Direkt Aggregate nutzen
            var result = df.Lazy()
                           .Aggregate(
                                new[] { Col("Cat"), Col("SubCat") },
                                new[] { Count().As("Cnt") }
                           )
                           .Collect();

            Assert.Equal(3, result.RowCount);

            var groupNull = result.Where(r => r.Get<string>("Cat") == "A" && r.IsNull("SubCat"));
            Assert.Equal(2, groupNull["Cnt"].Get<int>(0));
        }

        [Fact]
        public void GroupBy_ThreeColumns_MixedTypes_Works()
        {
            var df = DataFrame.FromObjects(new[]
            {
                new { A = "X", B = 1, C = true,  Val = 1 },
                new { A = "X", B = 1, C = true,  Val = 1 },
                new { A = "X", B = 1, C = false, Val = 1 },
                new { A = "X", B = 2, C = true,  Val = 1 },
                new { A = "Y", B = 1, C = true,  Val = 1 }
            });

            // FIX: Direkt Aggregate nutzen
            var result = df.Lazy()
                           .Aggregate(
                                new[] { Col("A"), Col("B"), Col("C") },
                                new[] { Count().As("Count") } // Alias explizit setzen für Test
                           )
                           .Collect();

            Assert.Equal(4, result.RowCount);

            var largeGroup = result.Where(r =>
                r.Get<string>("A") == "X" &&
                r.Get<int>("B") == 1 &&
                r.Get<bool>("C") == true);

            Assert.Equal(2, largeGroup["Count"].Get<int>(0));
        }

        [Fact]
        public void GroupBy_AllUnique_HighCardinality_NoCollisions()
        {
            int N = 1000;
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("ID1", typeof(int)),
                new ColumnDefinition("ID2", typeof(int))
            });
            var df = DataFrame.Create(schema, N);

            for (int i = 0; i < N; i++)
            {
                ((IntColumn)df["ID1"]).Append(i);
                ((IntColumn)df["ID2"]).Append(i * 2);
            }

            // FIX: Direkt Aggregate nutzen
            var result = df.Lazy()
                           .Aggregate(
                                new[] { Col("ID1"), Col("ID2") },
                                new[] { Count() }
                           )
                           .Collect();

            Assert.Equal(N, result.RowCount);

            var check = result.Where(r => r.Get<int>("ID1") == 500);
            Assert.Equal(1000, check["ID2"].Get<int>(0));
        }

        [Fact]
        public void GroupBy_EmptyDataFrame_ReturnsEmptyResult()
        {
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("A", typeof(int)),
                new ColumnDefinition("B", typeof(int))
            });
            var df = DataFrame.Create(schema, 0);

            // FIX: Direkt Aggregate nutzen
            var result = df.Lazy()
                           .Aggregate(
                                new[] { Col("A"), Col("B") },
                                new[] { Count() }
                           )
                           .Collect();

            Assert.Equal(0, result.RowCount);
            // Je nach Implementierung sind die Group-Cols dabei. Count ist sicher dabei.
            Assert.True(result.HasColumn("A"));
            Assert.True(result.HasColumn("B"));
        }
    }
}