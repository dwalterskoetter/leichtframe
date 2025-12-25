using LeichtFrame.Core.Plans;

namespace LeichtFrame.Core.Execution.Materialization.Executors
{
    internal class ScanExecutor : IPhysicalExecutor
    {
        private readonly DataFrameScan _node;

        public ScanExecutor(DataFrameScan node)
        {
            _node = node;
        }

        public DataFrame Execute() => _node.Source;
    }
}