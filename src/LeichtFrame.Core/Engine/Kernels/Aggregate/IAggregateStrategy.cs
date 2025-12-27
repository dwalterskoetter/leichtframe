using LeichtFrame.Core.Operations.Aggregate;

namespace LeichtFrame.Core.Engine.Kernels.Aggregate
{
    internal interface IAggregateStrategy
    {
        DataFrame Count(GroupedDataFrame gdf);
        DataFrame Sum(GroupedDataFrame gdf, string columnName);
        DataFrame Min(GroupedDataFrame gdf, string columnName);
        DataFrame Max(GroupedDataFrame gdf, string columnName);
        DataFrame Mean(GroupedDataFrame gdf, string columnName);
        DataFrame Aggregate(GroupedDataFrame gdf, AggregationDef[] aggregations);
    }
}