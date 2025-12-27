using System.Collections;

namespace LeichtFrame.Core.Execution.Streaming.Enumerators
{
    internal class DataFrameEnumerable : IEnumerable<RowView>
    {
        private readonly DataFrame _df;
        public DataFrameEnumerable(DataFrame df) { _df = df; }
        public IEnumerator<RowView> GetEnumerator()
        {
            for (int i = 0; i < _df.RowCount; i++)
                yield return new RowView(i, _df.Columns, _df.Schema);
        }
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}