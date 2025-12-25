using LeichtFrame.Core.Operations.Sort;
using LeichtFrame.Core.Plans;

namespace LeichtFrame.Core.Execution.Materialization.Executors
{
    internal class SortExecutor : IPhysicalExecutor
    {
        private readonly Sort _node;
        private readonly IPhysicalExecutor _input;

        public SortExecutor(Sort node, IPhysicalExecutor input)
        {
            _node = node;
            _input = input;
        }

        public DataFrame Execute()
        {
            var df = _input.Execute();
            string[] names = _node.SortColumns.Select(x => x.Name).ToArray();
            bool[] ascending = _node.SortColumns.Select(x => x.Ascending).ToArray();
            return OrderOps.OrderBy(df, names, ascending);
        }
    }
}