namespace LeichtFrame.Core.Engine.Kernels.GroupBy.Strategies
{
    internal class StringDictionaryStrategy : IGroupByStrategy
    {
        private const int MaxCardinality = 65_536;

        public unsafe GroupedDataFrame Group(DataFrame df, string columnName)
        {
            var col = (StringColumn)df[columnName];

            var dict = new Dictionary<string, int>(1024);
            int nextId = 0;

            int[] codes = new int[df.RowCount];
            bool aborted = false;
            bool hasNulls = false;
            int nullId = 0;
            int stringStartId = 1;

            var reverseMap = new List<string?>();
            reverseMap.Add(null);

            for (int i = 0; i < df.RowCount; i++)
            {
                if (col.IsNull(i))
                {
                    hasNulls = true;
                    codes[i] = nullId;
                    continue;
                }

                string? val = col.Get(i);

                if (!dict.TryGetValue(val!, out int id))
                {
                    if (nextId >= MaxCardinality)
                    {
                        aborted = true;
                        break;
                    }

                    id = stringStartId + nextId;
                    dict[val!] = id;
                    reverseMap.Add(val);
                    nextId++;
                }
                codes[i] = id;
            }

            if (aborted)
            {
                return new StringSwissMapStrategy().Group(df, columnName);
            }

            var codeCol = new IntColumn("Codes", codes, df.RowCount);

            var histogramStrategy = new DirectAddressingStrategy(min: 0, max: reverseMap.Count);

            NativeGroupedData nativeResult = histogramStrategy.ComputeNative(codeCol, df.RowCount);

            return new DictionaryGroupedDataFrame(
                df,
                new[] { columnName },
                nativeResult,
                reverseMap.ToArray(),
                hasNullCodeZero: hasNulls
            );
        }
    }
}