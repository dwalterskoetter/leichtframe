using System;
using System.Collections.Generic;
using System.Linq;

namespace LeichtFrame.Core
{
    /// <summary>
    /// Provides extension methods for joining multiple <see cref="DataFrame"/> objects.
    /// </summary>
    public static class DataFrameJoinExtensions
    {
        /// <summary>
        /// Joins two DataFrames based on a common key column using a Hash Join algorithm.
        /// </summary>
        /// <param name="left">The left DataFrame (the source of the method call).</param>
        /// <param name="right">The right DataFrame to join against.</param>
        /// <param name="on">The name of the column to use as the join key. Must exist in both DataFrames.</param>
        /// <param name="joinType">The type of join to perform (e.g., Inner). Currently, only <see cref="JoinType.Inner"/> is supported.</param>
        /// <returns>A new <see cref="DataFrame"/> containing columns from both sources, matched by the key.</returns>
        /// <exception cref="NotImplementedException">Thrown if <paramref name="joinType"/> is not Inner.</exception>
        /// <exception cref="NotSupportedException">Thrown if there are column name collisions between the two DataFrames.</exception>
        public static DataFrame Join(this DataFrame left, DataFrame right, string on, JoinType joinType = JoinType.Inner)
        {
            if (joinType != JoinType.Inner)
                throw new NotImplementedException("Only Inner Join is currently supported.");

            var leftKeyCol = left[on];
            var rightKeyCol = right[on];

            // 1. Build Phase: Create Hash Map from right table
            // Key: Cell value, Value: List of row indices
            var hashTable = new Dictionary<object, List<int>>();
            object nullSentinel = new object(); // Placeholder for null values

            for (int r = 0; r < right.RowCount; r++)
            {
                // Get value (handle null safely)
                object key = rightKeyCol.GetValue(r) ?? nullSentinel;

                if (!hashTable.TryGetValue(key, out var indices))
                {
                    indices = new List<int>();
                    hashTable[key] = indices;
                }
                indices.Add(r);
            }

            // 2. Probe Phase: Scan left table and find matches
            var leftIndices = new List<int>();
            var rightIndices = new List<int>();

            for (int l = 0; l < left.RowCount; l++)
            {
                object key = leftKeyCol.GetValue(l) ?? nullSentinel;

                if (hashTable.TryGetValue(key, out var matchingRightIndices))
                {
                    // Match found! (Can be 1:N)
                    foreach (var rIdx in matchingRightIndices)
                    {
                        leftIndices.Add(l);
                        rightIndices.Add(rIdx);
                    }
                }
            }

            // 3. Materialize Phase: Create new columns
            var newColumns = new List<IColumn>();

            // 3a. Copy all left columns (only matching rows)
            foreach (var col in left.Columns)
            {
                newColumns.Add(col.CloneSubset(leftIndices));
            }

            // 3b. Add right columns
            foreach (var col in right.Columns)
            {
                // Skip the join key column from the right (we already have it from the left)
                if (col.Name == on) continue;

                // Check for name conflicts (for MVP we throw an error)
                if (left.Schema.HasColumn(col.Name))
                {
                    throw new NotSupportedException(
                        $"Column name collision: '{col.Name}' exists in both DataFrames. Please rename columns before joining.");
                }

                newColumns.Add(col.CloneSubset(rightIndices));
            }

            return new DataFrame(newColumns);
        }
    }
}