using System.Reflection;

namespace LeichtFrame.Core.Engine
{
    internal class ShallowCopyRenameStrategy : IRenameStrategy
    {
        private static readonly FieldInfo? _nameField = typeof(Column).GetField("<Name>k__BackingField",
            BindingFlags.Instance | BindingFlags.NonPublic);

        public IColumn Rename(IColumn col, string newName)
        {
            if (col.Name == newName) return col;

            if (col is Column baseCol)
            {
                var newCol = baseCol.ShallowClone();

                if (_nameField != null)
                {
                    _nameField.SetValue(newCol, newName);
                }
                else
                {
                    throw new InvalidOperationException("Cannot rename: Name backing field not found.");
                }

                return newCol;
            }

            throw new NotSupportedException($"Rename not supported for type {col.GetType().Name}");
        }
    }
}