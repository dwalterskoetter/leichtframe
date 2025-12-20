using LeichtFrame.Core.Engine.Kernels.GroupBy.Strategies;

namespace LeichtFrame.Core.Engine.Kernels.GroupBy
{
    internal static class GroupByDispatcher
    {
        private const int MaxDirectMapRange = 10_000_000;

        public static GroupedDataFrame DecideAndExecute(DataFrame df, string columnName)
        {
            var col = df[columnName];
            int rowCount = df.RowCount;

            if (rowCount == 0) return new GenericHashMapStrategy().Group(df, columnName);

            if (col is IntColumn intCol)
            {
                int min = intCol.Min();
                int max = intCol.Max();
                long range = (long)max - min;

                // SCENARIO A: Fast Path (Native Histogram)
                if (range >= 0 && range <= MaxDirectMapRange)
                {
                    return new PolarsStyleLowCardStrategy(min, max).Group(df, columnName);
                }

                // SCENARIO B: High Cardinality / Sparse (z.B. Random IDs, Range > 1M)
                return new IntRadixStrategy().Group(df, columnName);
            }

            // 2. String Optimization
            if (col is StringColumn)
            {
                return new StringSmartStrategy().Group(df, columnName);
            }

            // 3. Fallback (Double, DateTime, Bool...)
            // TODO: Für Bool könnte man hier noch BoolDirectStrategy einbauen
            return new GenericHashMapStrategy().Group(df, columnName);
        }

        public static GroupedDataFrame DecideAndExecute(DataFrame df, string[] columnNames)
        {
            // Wenn nur 1 Spalte -> Fast Path über Single Column Logic
            if (columnNames.Length == 1)
            {
                return DecideAndExecute(df, columnNames[0]);
            }

            // Multi Column -> Unser neuer Kernel
            return new MultiColumnHashStrategy().Group(df, columnNames);
        }
    }
}