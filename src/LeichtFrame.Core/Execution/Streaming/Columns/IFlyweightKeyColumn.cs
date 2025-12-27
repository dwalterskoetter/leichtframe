namespace LeichtFrame.Core.Execution.Streaming.Columns
{
    internal interface IFlyweightKeyColumn : IColumn
    {
        void SetData(int keyOrIndex, bool isNull);
    }
}