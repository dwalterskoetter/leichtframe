using LeichtFrame.Core.Engine.Kernels.Aggregate.Strategies;
using LeichtFrame.Core.Operations.Aggregate;

namespace LeichtFrame.Core.Engine.Kernels.Aggregate
{
    internal static class AggregateDispatcher
    {
        private static readonly IAggregateStrategy _nativeStrategy = new NativeAggregateStrategy();
        private static readonly IAggregateStrategy _managedStrategy = new ManagedAggregateStrategy();

        private static bool CanUseNative(GroupedDataFrame gdf)
        {
            return gdf.NativeData != null && !gdf.KeysAreRowIndices;
        }

        public static DataFrame Count(GroupedDataFrame gdf)
        {
            if (CanUseNative(gdf)) return _nativeStrategy.Count(gdf);
            return _managedStrategy.Count(gdf);
        }

        public static DataFrame Sum(GroupedDataFrame gdf, string columnName)
        {
            if (CanUseNative(gdf))
            {
                var col = gdf.Source[columnName];
                if (col is IntColumn) return _nativeStrategy.Sum(gdf, columnName);
            }
            return _managedStrategy.Sum(gdf, columnName);
        }

        public static DataFrame Min(GroupedDataFrame gdf, string columnName)
        {
            if (CanUseNative(gdf))
            {
                var col = gdf.Source[columnName];
                if (col is IntColumn) return _nativeStrategy.Min(gdf, columnName);
            }
            return _managedStrategy.Min(gdf, columnName);
        }

        public static DataFrame Max(GroupedDataFrame gdf, string columnName)
        {
            if (CanUseNative(gdf))
            {
                var col = gdf.Source[columnName];
                if (col is IntColumn) return _nativeStrategy.Max(gdf, columnName);
            }
            return _managedStrategy.Max(gdf, columnName);
        }

        public static DataFrame Mean(GroupedDataFrame gdf, string columnName)
        {
            return _managedStrategy.Mean(gdf, columnName);
        }

        public static DataFrame Aggregate(GroupedDataFrame gdf, AggregationDef[] aggregations)
        {
            return _managedStrategy.Aggregate(gdf, aggregations);
        }
    }
}