using LeichtFrame.Core.Engine.Kernels.GroupBy.Strategies;

namespace LeichtFrame.Core.Engine.Kernels.GroupBy
{
    internal static class GroupByDispatcher
    {
        public static GroupedDataFrame DecideAndExecute(DataFrame df, string columnName)
        {
            var col = df[columnName];
            int rowCount = df.RowCount;

            if (rowCount == 0) return new GenericHashMapStrategy().Group(df, columnName);

            // --- INT ---
            if (col is IntColumn intCol)
            {
                int min = intCol.Min();
                int max = intCol.Max();
                long range = (long)max - min;

                // Dense -> Direct Addressing
                if (range >= 0 && range <= 1_000_000)
                {
                    return new DirectAddressingStrategy(min, max).Group(df, columnName);
                }
                // Sparse -> Int Swiss Map
                return new IntSwissMapStrategy().Group(df, columnName);
            }

            // --- STRING ---
            if (col is StringColumn strCol)
            {
                // Low Cardinality -> String Dictionary
                if (IsLikelyLowCardinality(strCol, rowCount))
                {
                    return new StringDictionaryStrategy().Group(df, columnName);
                }

                // High Cardinality -> String Swiss Map
                return new StringSwissMapStrategy().Group(df, columnName);
            }

            return new GenericHashMapStrategy().Group(df, columnName);
        }

        public static GroupedDataFrame DecideAndExecute(DataFrame df, string[] columnNames)
        {
            if (columnNames.Length == 1) return DecideAndExecute(df, columnNames[0]);

            bool allPackable = true;
            foreach (var c in columnNames)
            {
                var t = df[c].DataType;
                t = Nullable.GetUnderlyingType(t) ?? t;
                if (t != typeof(int) && t != typeof(double) && t != typeof(bool) && t != typeof(long) && t != typeof(DateTime))
                    allPackable = false;
            }

            if (allPackable)
            {
                // Fixed-Width Primitives -> Row Layout Packing
                return new RowLayoutHashStrategy().Group(df, columnNames);
            }

            // Fallback: Mixed Types / Variable Length -> Component-wise Hashing
            return new MultiColumnHashStrategy().Group(df, columnNames);
        }

        // Helper: Detect Low Cardinality via Sampling
        private static bool IsLikelyLowCardinality(StringColumn col, int rowCount)
        {
            if (rowCount < 1024) return false;

            int sampleSize = 64;
            var set = new HashSet<string>();

            int step = rowCount / sampleSize;
            for (int i = 0; i < rowCount; i += step)
            {
                string? val = col.Get(i);
                if (val != null) set.Add(val);

                if (set.Count > sampleSize * 0.8) return false;
            }
            return true;
        }
    }
}