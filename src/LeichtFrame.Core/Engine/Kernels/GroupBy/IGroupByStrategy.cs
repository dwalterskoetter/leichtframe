namespace LeichtFrame.Core.Engine
{
    internal interface IGroupByStrategy
    {
        GroupedDataFrame Group(DataFrame frame, string columnName);
    }
}