namespace LeichtFrame.Core.Execution.Materialization
{
    /// <summary>
    /// Represents an executable unit in the physical plan.
    /// </summary>
    internal interface IPhysicalExecutor
    {
        DataFrame Execute();
    }
}