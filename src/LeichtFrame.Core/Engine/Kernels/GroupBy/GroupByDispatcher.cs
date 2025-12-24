using LeichtFrame.Core.Engine.Kernels.GroupBy.Strategies;
using LeichtFrame.Core.Engine.Algorithms.Converter;

namespace LeichtFrame.Core.Engine.Kernels.GroupBy
{
    internal static class GroupByDispatcher
    {
        public static GroupedDataFrame DecideAndExecute(DataFrame df, string columnName)
        {
            var col = df[columnName];
            int rowCount = df.RowCount;

            if (rowCount == 0) return new GenericHashMapStrategy().Group(df, columnName);

            if (col is CategoryColumn catCol)
            {
                // Super-Fast Path: Direct Addressing on integer codes.
                var strategy = new DirectAddressingStrategy(min: 0, max: catCol.Cardinality);
                var nativeData = strategy.ComputeNative(catCol.Codes, df.RowCount);

                return new DictionaryGroupedDataFrame(
                    df,
                    new[] { columnName },
                    nativeData,
                    catCol.DictionaryArray,
                    hasNullCodeZero: true
                );
            }

            // --- 2. INT ---
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

            // --- 3. STRING ---
            if (col is StringColumn strCol)
            {
                // Low Cardinality -> Auto-Convert to Category -> String Dictionary
                if (IsLikelyLowCardinality(strCol, rowCount))
                {
                    using var autoCatCol = ConvertStringColumnToCategory(strCol);

                    var strategy = new DirectAddressingStrategy(min: 0, max: autoCatCol.Cardinality);
                    var nativeData = strategy.ComputeNative(autoCatCol.Codes, rowCount);

                    return new DictionaryGroupedDataFrame(
                        df,
                        new[] { columnName },
                        nativeData,
                        autoCatCol.DictionaryArray,
                        hasNullCodeZero: true
                    );
                }

                // High Cardinality -> String Swiss Map
                return new StringSwissMapStrategy().Group(df, columnName);
            }

            // --- 4. FALLBACK (Generic) ---
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
            if (rowCount == 0) return false;

            int sampleSize = 128;
            var set = new HashSet<string>();
            int step = rowCount / sampleSize;
            if (step == 0) step = 1;

            for (int i = 0; i < rowCount; i += step)
            {
                if (set.Count >= sampleSize) break;

                string? val = col.Get(i);
                if (val != null) set.Add(val);

                // If sample has > 50% unique values -> High Card
                if (set.Count > (sampleSize * 0.5)) return false;
            }
            return true;
        }

        // Helper: Convert String Column To Category Column using optimized Unsafe Converter
        private static CategoryColumn ConvertStringColumnToCategory(StringColumn sc)
        {
            var cat = ParallelStringConverter.Convert(sc);
            return cat;
        }
    }
}