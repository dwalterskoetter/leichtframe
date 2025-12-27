using LeichtFrame.Core.Engine.Kernels.Aggregate;

namespace LeichtFrame.Core.Operations.Aggregate
{
    /// <summary>
    /// Group Aggregation Extensions
    /// </summary>
    public static class GroupAggregationExtensions
    {
        /// <summary>
        /// Count Extension
        /// </summary>
        /// <param name="gdf"></param>
        /// <returns></returns>
        public static DataFrame Count(this GroupedDataFrame gdf) => AggregateDispatcher.Count(gdf);

        /// <summary>
        /// Sum Extension
        /// </summary>
        /// <param name="gdf"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public static DataFrame Sum(this GroupedDataFrame gdf, string columnName)
            => AggregateDispatcher.Sum(gdf, columnName);

        /// <summary>
        /// Min Extension
        /// </summary>
        /// <param name="gdf"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public static DataFrame Min(this GroupedDataFrame gdf, string columnName)
            => AggregateDispatcher.Min(gdf, columnName);

        /// <summary>
        /// Max Extension
        /// </summary>
        /// <param name="gdf"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public static DataFrame Max(this GroupedDataFrame gdf, string columnName)
            => AggregateDispatcher.Max(gdf, columnName);

        /// <summary>
        /// Mean Extension
        /// </summary>
        /// <param name="gdf"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public static DataFrame Mean(this GroupedDataFrame gdf, string columnName)
            => AggregateDispatcher.Mean(gdf, columnName);

        /// <summary>
        /// Aggregate Extension
        /// </summary>
        /// <param name="gdf"></param>
        /// <param name="aggregations"></param>
        /// <returns></returns>
        public static DataFrame Aggregate(this GroupedDataFrame gdf, params AggregationDef[] aggregations)
            => AggregateDispatcher.Aggregate(gdf, aggregations);
    }
}