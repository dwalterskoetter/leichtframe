namespace LeichtFrame.Core
{
    internal interface IGroupByStrategy
    {
        GroupedDataFrame Group(DataFrame frame, string columnName);
    }
}