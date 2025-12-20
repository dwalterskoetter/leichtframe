using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;
using DuckDB.NET.Data;
using static LeichtFrame.Core.Expressions.F;

namespace LeichtFrame.Benchmarks
{
    [MemoryDiagnoser]
    public class LazyAdvancedBenchmarks : BenchmarkData
    {
        private DataFrame _products = null!;
        private DataFrame _orders = null!;

        public record ProductPoco(int ProductId, string Category);
        public record OrderPoco(int OrderId, int ProductId, double Amount);

        /// <summary>
        /// Setup specifically for advanced lazy benchmarks.
        /// Overrides base setup to generate relational data (Orders/Products).
        /// </summary>
        public override void GlobalSetup()
        {
            base.GlobalSetup();

            int productCount = N / 10;
            var prodList = new List<ProductPoco>(productCount);
            for (int i = 0; i < productCount; i++)
            {
                prodList.Add(new ProductPoco(i, $"Cat_{i % 5}"));
            }
            _products = DataFrame.FromObjects(prodList);

            var rnd = new Random(42);
            var orderList = new List<OrderPoco>(N);
            for (int i = 0; i < N; i++)
            {
                orderList.Add(new OrderPoco(i, rnd.Next(0, productCount), _pocoList[i].Val));
            }
            _orders = DataFrame.FromObjects(orderList);

            using var cmd = _duckConnection.CreateCommand();

            cmd.CommandText = "CREATE TABLE Products (ProductId INTEGER, Category VARCHAR)";
            cmd.ExecuteNonQuery();

            cmd.CommandText = "CREATE TABLE Orders (OrderId INTEGER, ProductId INTEGER, Amount DOUBLE)";
            cmd.ExecuteNonQuery();

            using (var appender = _duckConnection.CreateAppender("Products"))
            {
                foreach (var p in prodList)
                {
                    var row = appender.CreateRow();
                    row.AppendValue(p.ProductId);
                    row.AppendValue(p.Category);
                    row.EndRow();
                }
            }

            using (var appender = _duckConnection.CreateAppender("Orders"))
            {
                foreach (var o in orderList)
                {
                    var row = appender.CreateRow();
                    row.AppendValue(o.OrderId);
                    row.AppendValue(o.ProductId);
                    row.AppendValue(o.Amount);
                    row.EndRow();
                }
            }
        }

        // =========================================================
        // QUERY:
        // SELECT Category, SUM(Amount) as Total, COUNT(*) as Count
        // FROM Orders o
        // JOIN Products p ON o.ProductId = p.ProductId
        // WHERE Amount > 500.0
        // GROUP BY Category
        // ORDER BY Total DESC
        // =========================================================

        [Benchmark(Baseline = true, Description = "DuckDB SQL")]
        public void DuckDB_Complex()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = @"
                SELECT p.Category, SUM(o.Amount) as Total, COUNT(*) as Cnt
                FROM Orders o
                JOIN Products p ON o.ProductId = p.ProductId
                WHERE o.Amount > 500.0
                GROUP BY p.Category
                ORDER BY Total DESC";

            using var reader = cmd.ExecuteReader();
            while (reader.Read()) { }
        }

        [Benchmark(Description = "LF Eager (Classic)")]
        public DataFrame LF_Eager()
        {
            var filtered = _orders.Where(r => r.Get<double>("Amount") > 500.0);

            var joined = filtered.Join(_products, "ProductId", JoinType.Inner);

            using var grouped = joined.GroupBy("Category");

            var agg = grouped.Aggregate(
                Agg.Sum("Amount", "Total"),
                Agg.Count("Cnt")
            );

            return agg.OrderByDescending("Total");
        }

        [Benchmark(Description = "LF Lazy (New Engine)")]
        public DataFrame LF_Lazy()
        {
            return _orders.Lazy()
                .Where(Col("Amount") > 500.0)
                .Join(_products.Lazy(), "ProductId")
                .GroupBy("Category",
                    Sum(Col("Amount")).As("Total"),
                    Count().As("Cnt")
                )
                .OrderByDescending("Total")
                .Collect();
        }
    }
}