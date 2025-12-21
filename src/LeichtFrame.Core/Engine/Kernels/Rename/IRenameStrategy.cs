namespace LeichtFrame.Core.Engine
{
    internal interface IRenameStrategy
    {
        /// <summary>
        /// Renames a column. Should aim for Zero-Copy (Shallow Clone) if possible.
        /// </summary>
        IColumn Rename(IColumn col, string newName);
    }
}