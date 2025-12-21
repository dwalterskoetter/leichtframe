namespace LeichtFrame.Core.Engine.Kernels.Rename
{
    /// <summary>
    /// Rename Dispatcher
    /// </summary>
    public static class RenameDispatcher
    {
        private static readonly IRenameStrategy _strategy = new ShallowCopyRenameStrategy();

        /// <summary>
        /// Execute rename
        /// </summary>
        /// <param name="col"></param>
        /// <param name="newName"></param>
        /// <returns></returns>
        public static IColumn Execute(IColumn col, string newName)
        {
            return _strategy.Rename(col, newName);
        }
    }
}