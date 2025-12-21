using LeichtFrame.Core.Engine.Kernels.Rename;

namespace LeichtFrame.Core.Operations.Transform
{
    /// <summary>
    /// Executes Rename Operations using Engine
    /// </summary>
    public static class RenameOps
    {
        /// <summary>
        /// /// Executes Rename Operations using Engine
        /// </summary>
        /// <param name="col"></param>
        /// <param name="newName"></param>
        /// <returns></returns>
        public static IColumn Rename(this IColumn col, string newName)
        {
            return RenameDispatcher.Execute(col, newName);
        }
    }
}