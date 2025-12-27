using LeichtFrame.Core.Plans;
using static LeichtFrame.Core.Expressions.F;

namespace LeichtFrame.Core.Tests.Lazy
{
    public class LazyGroupingTests
    {
        private DataFrame GetDummyDataFrame()
        {
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Category", typeof(string)),
                new ColumnDefinition("Value", typeof(double))
            });
            return DataFrame.Create(schema, 0);
        }

        [Fact]
        public void GroupBy_Strings_Returns_GroupedLazyFrame()
        {
            var df = GetDummyDataFrame();

            // Act
            var grouped = df.Lazy().GroupBy("Category", "Id");

            // Assert
            Assert.NotNull(grouped);
            Assert.IsType<GroupedLazyFrame>(grouped);
        }

        [Fact]
        public void GroupBy_Expressions_Returns_GroupedLazyFrame()
        {
            var df = GetDummyDataFrame();

            // Act
            var grouped = df.Lazy().GroupBy(Col("Category"), Col("Id"));

            // Assert
            Assert.NotNull(grouped);
            Assert.IsType<GroupedLazyFrame>(grouped);
        }

        [Fact]
        public void Agg_Creates_Correct_LogicalPlan_AggregateNode()
        {
            var df = GetDummyDataFrame();

            // Act: Build the plan
            var lazyResult = df.Lazy()
                               .GroupBy("Category")
                               .Agg(
                                   Sum(Col("Value")).As("SumVal"),
                                   Count().As("Cnt")
                               );

            // Assert: Inspect the Plan
            Assert.NotNull(lazyResult.Plan);

            // The root node should be an Aggregate node
            var aggNode = Assert.IsType<Aggregate>(lazyResult.Plan);

            // Check Grouping Keys
            Assert.Single(aggNode.GroupExprs);
            var groupKey = Assert.IsType<ColExpr>(aggNode.GroupExprs[0]);
            Assert.Equal("Category", groupKey.Name);

            // Check Aggregations
            Assert.Equal(2, aggNode.AggExprs.Count);

            // Check Agg 1: Sum(Value) as SumVal
            var agg1 = Assert.IsType<AliasExpr>(aggNode.AggExprs[0]);
            Assert.Equal("SumVal", agg1.Alias);
            var sumExpr = Assert.IsType<AggExpr>(agg1.Child);
            Assert.Equal(AggOpType.Sum, sumExpr.Op);

            // Check Agg 2: Count() as Cnt
            var agg2 = Assert.IsType<AliasExpr>(aggNode.AggExprs[1]);
            Assert.Equal("Cnt", agg2.Alias);
            var countExpr = Assert.IsType<AggExpr>(agg2.Child);
            Assert.Equal(AggOpType.Count, countExpr.Op);
        }

        [Fact]
        public void Agg_Chains_Correctly_After_Where()
        {
            var df = GetDummyDataFrame();

            // Plan: Scan -> Filter -> Aggregate
            var lazyResult = df.Lazy()
                               .Where(Col("Id") > 10)
                               .GroupBy("Category")
                               .Agg(Count());

            var aggNode = Assert.IsType<Aggregate>(lazyResult.Plan);
            var filterNode = Assert.IsType<Filter>(aggNode.Input);
            var scanNode = Assert.IsType<DataFrameScan>(filterNode.Input);

            Assert.Same(df, scanNode.Source);
        }

        [Fact]
        public void MultiColumn_GroupBy_Plan_Is_Correct()
        {
            var df = GetDummyDataFrame();

            var lazyResult = df.Lazy()
                               .GroupBy("Category", "Id")
                               .Agg(Count());

            var aggNode = Assert.IsType<Aggregate>(lazyResult.Plan);

            Assert.Equal(2, aggNode.GroupExprs.Count);
            Assert.Equal("Category", ((ColExpr)aggNode.GroupExprs[0]).Name);
            Assert.Equal("Id", ((ColExpr)aggNode.GroupExprs[1]).Name);
        }
    }
}