namespace LeichtFrame.Core.Operations.Delegates
{
    internal static class FilterDelegateOps
    {
        public static DataFrame Execute(DataFrame df, Func<RowView, bool> predicate)
        {
            if (predicate == null) throw new ArgumentNullException(nameof(predicate));

            var indices = new List<int>(df.RowCount / 2);

            for (int i = 0; i < df.RowCount; i++)
            {
                var row = new RowView(i, df.Columns, df.Schema);
                if (predicate(row))
                {
                    indices.Add(i);
                }
            }

            var newColumns = new List<IColumn>(df.ColumnCount);
            foreach (var col in df.Columns)
            {
                newColumns.Add(col.CloneSubset(indices));
            }

            return new DataFrame(newColumns);
        }
    }
}