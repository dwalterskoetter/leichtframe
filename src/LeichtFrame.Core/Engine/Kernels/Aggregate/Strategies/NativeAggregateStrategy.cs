using LeichtFrame.Core.Operations.Aggregate;

namespace LeichtFrame.Core.Engine.Kernels.Aggregate.Strategies
{
    internal class NativeAggregateStrategy : IAggregateStrategy
    {
        public DataFrame Count(GroupedDataFrame gdf)
        {
            return NativeCountExecutor.Execute(gdf, gdf.NativeData!);
        }

        public DataFrame Sum(GroupedDataFrame gdf, string columnName)
        {
            var col = (IntColumn)gdf.Source[columnName];
            return NativeSumExecutor.Execute(gdf, gdf.NativeData!, col, columnName);
        }

        public DataFrame Min(GroupedDataFrame gdf, string columnName)
        {
            var col = (IntColumn)gdf.Source[columnName];
            return NativeMinMaxExecutor.Execute(gdf, gdf.NativeData!, col, columnName, isMin: true);
        }

        public DataFrame Max(GroupedDataFrame gdf, string columnName)
        {
            var col = (IntColumn)gdf.Source[columnName];
            return NativeMinMaxExecutor.Execute(gdf, gdf.NativeData!, col, columnName, isMin: false);
        }

        public DataFrame Mean(GroupedDataFrame gdf, string columnName)
        {
            throw new NotSupportedException("Native Mean not implemented yet.");
        }

        public DataFrame Aggregate(GroupedDataFrame gdf, AggregationDef[] aggregations)
        {
            throw new NotSupportedException("Native Multi-Aggregate not implemented yet.");
        }
    }
}