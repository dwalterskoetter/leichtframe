using LeichtFrame.Core;
using LeichtFrame.Core.Expressions;
using static LeichtFrame.Core.Expressions.F; // Importiert Col, Lit, Sum, etc.

namespace LeichtFrame.Core.Tests.Lazy
{
    public class LazyAdvancedOpsTests
    {
        // =======================================================================
        // 1. GROUP BY & AGGREGATION TESTS
        // =======================================================================

        [Fact]
        public void GroupBy_Sum_Calculates_Correctly()
        {
            // Arrange
            var df = DataFrame.FromObjects(new[]
            {
                new { Dept = "IT", Salary = 5000 },
                new { Dept = "IT", Salary = 4000 },
                new { Dept = "HR", Salary = 3000 }
            });

            // Act: SELECT Dept, SUM(Salary) as Total FROM df GROUP BY Dept
            var result = df.Lazy()
                           .GroupBy("Dept", Sum(Col("Salary")).As("Total"))
                           .Collect();

            // Assert
            Assert.Equal(2, result.RowCount);

            // Check IT Group (5000 + 4000 = 9000)
            var itRow = result.Where(r => r.Get<string>("Dept") == "IT");
            Assert.Equal(1, itRow.RowCount);
            Assert.Equal(9000.0, itRow["Total"].Get<double>(0));

            // Check HR Group (3000)
            var hrRow = result.Where(r => r.Get<string>("Dept") == "HR");
            Assert.Equal(3000.0, hrRow["Total"].Get<double>(0));
        }

        [Fact]
        public void GroupBy_Multiple_Aggregations_In_One_Pass()
        {
            // Arrange
            var df = DataFrame.FromObjects(new[]
            {
                new { Id = 1, Val = 10 },
                new { Id = 1, Val = 20 },
                new { Id = 1, Val = 30 }
            });

            // Act: Min, Max, Count, Mean
            var result = df.Lazy()
                           .GroupBy("Id",
                               Min(Col("Val")).As("MinVal"),
                               Max(Col("Val")).As("MaxVal"),
                               Count().As("Count"),
                               Mean(Col("Val")).As("Avg")
                           )
                           .Collect();

            // Assert
            Assert.Equal(1, result.RowCount);

            Assert.Equal(10, result["MinVal"].Get<int>(0)); // Min (Int)
            Assert.Equal(30, result["MaxVal"].Get<int>(0)); // Max (Int)

            Assert.Equal(3, result["Count"].Get<int>(0));        // Count
            Assert.Equal(20.0, result["Avg"].Get<double>(0));    // Mean
        }

        // =======================================================================
        // 2. JOIN TESTS
        // =======================================================================

        [Fact]
        public void Join_Inner_Matches_Intersection()
        {
            // Arrange
            var left = DataFrame.FromObjects(new[]
            {
                new { Key = 1, LeftVal = "L1" },
                new { Key = 2, LeftVal = "L2" },
                new { Key = 3, LeftVal = "L3" }
            });

            var right = DataFrame.FromObjects(new[]
            {
                new { Key = 2, RightVal = "R2" },
                new { Key = 3, RightVal = "R3" },
                new { Key = 4, RightVal = "R4" }
            });

            // Act
            var result = left.Lazy()
                             .Join(right.Lazy(), "Key", JoinType.Inner)
                             .Collect();

            // Assert (Expected: Keys 2 and 3)
            Assert.Equal(2, result.RowCount);

            // Verify content via sorting to ensure order
            var sorted = result.OrderBy("Key");
            Assert.Equal(2, sorted["Key"].Get<int>(0));
            Assert.Equal("L2", sorted["LeftVal"].Get<string>(0));
            Assert.Equal("R2", sorted["RightVal"].Get<string>(0));
        }

        [Fact]
        public void Join_Left_Preserves_Left_Side_And_Nulls_Right()
        {
            // Arrange
            var left = DataFrame.FromObjects(new[] { new { Id = 1 } });
            var right = DataFrame.FromObjects(new[] { new { Id = 2, Info = "Missing" } });

            // Act
            var result = left.Lazy()
                             .Join(right.Lazy(), "Id", JoinType.Left)
                             .Collect();

            // Assert
            Assert.Equal(1, result.RowCount);
            Assert.Equal(1, result["Id"].Get<int>(0));

            // Right column "Info" should be present but Null
            Assert.True(result.HasColumn("Info"));
            Assert.True(result["Info"].IsNull(0));
        }

        // =======================================================================
        // 3. SORTING TESTS
        // =======================================================================

        [Fact]
        public void OrderBy_Sorts_Ascending()
        {
            var df = DataFrame.FromObjects(new[] { new { A = 5 }, new { A = 1 }, new { A = 10 } });

            var result = df.Lazy()
                           .OrderBy("A")
                           .Collect();

            Assert.Equal(1, result["A"].Get<int>(0));
            Assert.Equal(5, result["A"].Get<int>(1));
            Assert.Equal(10, result["A"].Get<int>(2));
        }

        [Fact]
        public void OrderByDescending_Sorts_Descending()
        {
            var df = DataFrame.FromObjects(new[] { new { A = 1 }, new { A = 5 }, new { A = 10 } });

            var result = df.Lazy()
                           .OrderByDescending("A")
                           .Collect();

            Assert.Equal(10, result["A"].Get<int>(0));
            Assert.Equal(5, result["A"].Get<int>(1));
            Assert.Equal(1, result["A"].Get<int>(2));
        }

        // =======================================================================
        // 4. COMPLEX CHAINING (INTEGRATION)
        // =======================================================================

        [Fact]
        public void Complex_Query_Chains_Filter_Join_Group_Sort()
        {
            // Scenario: Sales Report
            // 1. Filter Orders (Amount > 10)
            // 2. Join with Products
            // 3. Group by Category
            // 4. Sum Amounts
            // 5. Order by Total Amount Descending

            // Data
            var orders = DataFrame.FromObjects(new[] {
                new { ProdId = 1, Amount = 100.0 }, // Cat A
                new { ProdId = 1, Amount = 50.0 },  // Cat A
                new { ProdId = 2, Amount = 5.0 },   // Cat B (Filtered out later)
                new { ProdId = 3, Amount = 200.0 }  // Cat C
            });

            var products = DataFrame.FromObjects(new[] {
                new { ProdId = 1, Category = "A" },
                new { ProdId = 2, Category = "B" },
                new { ProdId = 3, Category = "C" }
            });

            // Act
            var result = orders.Lazy()
                .Where(Col("Amount") > 10.0) // Filter out the 5.0
                .Join(products.Lazy(), "ProdId")
                .GroupBy("Category",
                    Sum(Col("Amount")).As("TotalSales")
                )
                .OrderByDescending("TotalSales")
                .Collect();

            // Assert
            // Expected Groups: 
            // - Cat C: 200.0
            // - Cat A: 150.0
            // - Cat B: Removed by filter

            Assert.Equal(2, result.RowCount);

            // Row 0 (Highest Sales) -> C
            Assert.Equal("C", result["Category"].Get<string>(0));
            Assert.Equal(200.0, result["TotalSales"].Get<double>(0));

            // Row 1 -> A
            Assert.Equal("A", result["Category"].Get<string>(1));
            Assert.Equal(150.0, result["TotalSales"].Get<double>(1));
        }
    }
}