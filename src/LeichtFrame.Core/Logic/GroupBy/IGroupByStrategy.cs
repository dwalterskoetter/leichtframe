namespace LeichtFrame.Core.Logic
{
    internal interface IGroupByStrategy
    {
        GroupedDataFrame Group(DataFrame frame, string columnName);
    }
}