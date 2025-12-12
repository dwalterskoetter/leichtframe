namespace LeichtFrame.Core
{
    /// <summary>
    /// Provides extension methods for joining multiple <see cref="DataFrame"/> objects.
    /// </summary>
    public static class DataFrameJoinExtensions
    {
        /// <summary>
        /// Joins two DataFrames based on a common key column using a Hash Join algorithm.
        /// Supports Inner and Left joins.
        /// </summary>
        /// <param name="left">The left DataFrame.</param>
        /// <param name="right">The right DataFrame.</param>
        /// <param name="on">The join key column.</param>
        /// <param name="joinType">The type of join (Inner or Left).</param>
        public static DataFrame Join(this DataFrame left, DataFrame right, string on, JoinType joinType = JoinType.Inner)
        {
            var leftKeyCol = left[on];
            var rightKeyCol = right[on];

            // 1. Build Phase: Create Hash Map from right table
            // Key: Cell value, Value: List of row indices in 'right'
            var hashTable = new Dictionary<object, List<int>>();
            object nullSentinel = new object();

            for (int r = 0; r < right.RowCount; r++)
            {
                object key = rightKeyCol.GetValue(r) ?? nullSentinel;
                if (!hashTable.TryGetValue(key, out var indices))
                {
                    indices = new List<int>();
                    hashTable[key] = indices;
                }
                indices.Add(r);
            }

            // 2. Probe Phase: Scan left table
            // We store indices. For Right side, -1 indicates "No Match" (for Left Join).
            var leftIndices = new List<int>(left.RowCount);
            var rightIndices = new List<int>(left.RowCount);

            for (int l = 0; l < left.RowCount; l++)
            {
                object key = leftKeyCol.GetValue(l) ?? nullSentinel;

                if (hashTable.TryGetValue(key, out var matchingRightIndices))
                {
                    // Match(es) found -> Add all combinations (Cartesian product for duplicates)
                    foreach (var rIdx in matchingRightIndices)
                    {
                        leftIndices.Add(l);
                        rightIndices.Add(rIdx);
                    }
                }
                else if (joinType == JoinType.Left)
                {
                    // No Match -> Preserve Left row, Right is missing (-1)
                    leftIndices.Add(l);
                    rightIndices.Add(-1);
                }
                // Else (Inner Join): Skip row
            }

            // 3. Materialize Phase
            var newColumns = new List<IColumn>();

            // 3a. Left Columns (Direct Subset)
            foreach (var col in left.Columns)
            {
                newColumns.Add(col.CloneSubset(leftIndices));
            }

            // 3b. Right Columns (Handle -1 and enforce Nullability)
            foreach (var col in right.Columns)
            {
                if (col.Name == on) continue; // Skip join key

                if (left.Schema.HasColumn(col.Name))
                {
                    throw new NotSupportedException(
                        $"Column name collision: '{col.Name}' exists in both DataFrames.");
                }

                // If Left Join, right columns MUST be nullable to hold the missing values.
                bool forceNullable = joinType == JoinType.Left || col.IsNullable;

                // Helper to create the new column with nulls where index is -1
                IColumn newCol = MaterializeRightColumn(col, rightIndices, forceNullable);
                newColumns.Add(newCol);
            }

            return new DataFrame(newColumns);
        }

        private static IColumn MaterializeRightColumn(IColumn source, List<int> indices, bool isNullable)
        {
            // We use ColumnFactory to create the target column (correctly typed)
            IColumn newCol = ColumnFactory.Create(source.Name, source.DataType, indices.Count, isNullable);

            // Optimization: Dispatch by type to avoid boxing in the loop
            if (source is IntColumn ic && newCol is IntColumn nic)
            {
                foreach (int idx in indices)
                {
                    if (idx == -1) nic.Append(null);
                    else if (ic.IsNull(idx)) nic.Append(null);
                    else nic.Append(ic.Get(idx));
                }
            }
            else if (source is DoubleColumn dc && newCol is DoubleColumn ndc)
            {
                foreach (int idx in indices)
                {
                    if (idx == -1) ndc.Append(null);
                    else if (dc.IsNull(idx)) ndc.Append(null);
                    else ndc.Append(dc.Get(idx));
                }
            }
            else if (source is StringColumn sc && newCol is StringColumn nsc)
            {
                foreach (int idx in indices)
                {
                    if (idx == -1) nsc.Append(null);
                    else nsc.Append(sc.Get(idx)); // Get handles null internally usually, but IsNull check is safe
                }
            }
            else if (source is BoolColumn bc && newCol is BoolColumn nbc)
            {
                foreach (int idx in indices)
                {
                    if (idx == -1) nbc.Append(null);
                    else if (bc.IsNull(idx)) nbc.Append(null);
                    else nbc.Append(bc.Get(idx));
                }
            }
            else if (source is DateTimeColumn dtc && newCol is DateTimeColumn ndtc)
            {
                foreach (int idx in indices)
                {
                    if (idx == -1) ndtc.Append(null);
                    else if (dtc.IsNull(idx)) ndtc.Append(null);
                    else ndtc.Append(dtc.Get(idx));
                }
            }
            else
            {
                // Fallback for unknown types (Boxing)
                foreach (int idx in indices)
                {
                    if (idx == -1) newCol.AppendObject(null);
                    else newCol.AppendObject(source.GetValue(idx));
                }
            }

            return newCol;
        }
    }
}