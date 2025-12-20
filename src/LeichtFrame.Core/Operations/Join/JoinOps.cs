namespace LeichtFrame.Core.Operations.Join
{
    /// <summary>
    /// Provides extension methods for joining multiple <see cref="DataFrame"/> objects.
    /// Optimized with typed HashMaps to avoid boxing overhead.
    /// </summary>
    public static class DataFrameJoinExtensions
    {
        /// <summary>
        /// Joins two DataFrames based on a common key column using a Hash Join algorithm.
        /// Supports Inner and Left joins.
        /// </summary>
        public static DataFrame Join(this DataFrame left, DataFrame right, string on, JoinType joinType = JoinType.Inner)
        {
            Type type = left[on].DataType;

            Type coreType = Nullable.GetUnderlyingType(type) ?? type;

            if (coreType == typeof(int))
                return ExecuteJoin<int>(left, right, on, joinType);

            if (coreType == typeof(double))
                return ExecuteJoin<double>(left, right, on, joinType);

            if (coreType == typeof(string))
                return ExecuteJoin<string>(left, right, on, joinType);

            if (coreType == typeof(bool))
                return ExecuteJoin<bool>(left, right, on, joinType);

            if (coreType == typeof(DateTime))
                return ExecuteJoin<DateTime>(left, right, on, joinType);

            return ExecuteJoin<object>(left, right, on, joinType);
        }

        private static DataFrame ExecuteJoin<T>(DataFrame left, DataFrame right, string on, JoinType joinType)
            where T : notnull
        {
            var leftKeyCol = (IColumn<T>)left[on];
            var rightKeyCol = (IColumn<T>)right[on];

            var hashTable = new Dictionary<T, List<int>>();

            for (int r = 0; r < right.RowCount; r++)
            {
                if (rightKeyCol.IsNull(r)) continue;

                T key = rightKeyCol.GetValue(r);

                if (key == null) continue;

                if (!hashTable.TryGetValue(key, out var indices))
                {
                    indices = new List<int>();
                    hashTable[key] = indices;
                }
                indices.Add(r);
            }

            var leftIndices = new List<int>(left.RowCount);
            var rightIndices = new List<int>(left.RowCount);

            for (int l = 0; l < left.RowCount; l++)
            {
                if (leftKeyCol.IsNull(l))
                {
                    if (joinType == JoinType.Left)
                    {
                        leftIndices.Add(l);
                        rightIndices.Add(-1);
                    }
                    continue;
                }

                T key = leftKeyCol.GetValue(l);
                if (key == null)
                {
                    if (joinType == JoinType.Left) { leftIndices.Add(l); rightIndices.Add(-1); }
                    continue;
                }

                if (hashTable.TryGetValue(key, out var matchingRightIndices))
                {
                    foreach (var rIdx in matchingRightIndices)
                    {
                        leftIndices.Add(l);
                        rightIndices.Add(rIdx);
                    }
                }
                else if (joinType == JoinType.Left)
                {
                    leftIndices.Add(l);
                    rightIndices.Add(-1);
                }
            }

            return MaterializeResult(left, right, on, leftIndices, rightIndices, joinType);
        }

        private static DataFrame MaterializeResult(
            DataFrame left,
            DataFrame right,
            string on,
            List<int> leftIndices,
            List<int> rightIndices,
            JoinType joinType)
        {
            var newColumns = new List<IColumn>();

            foreach (var col in left.Columns)
            {
                newColumns.Add(col.CloneSubset(leftIndices));
            }

            foreach (var col in right.Columns)
            {
                if (col.Name == on) continue;

                if (left.Schema.HasColumn(col.Name))
                    throw new NotSupportedException($"Column collision: '{col.Name}' exists in both DataFrames.");

                bool forceNullable = joinType == JoinType.Left || col.IsNullable;
                IColumn newCol = MaterializeRightColumn(col, rightIndices, forceNullable);
                newColumns.Add(newCol);
            }

            return new DataFrame(newColumns);
        }

        private static IColumn MaterializeRightColumn(IColumn source, List<int> indices, bool isNullable)
        {
            IColumn newCol = ColumnFactory.Create(source.Name, source.DataType, indices.Count, isNullable);

            if (source is IntColumn ic && newCol is IntColumn nic)
            {
                for (int i = 0; i < indices.Count; i++)
                {
                    int idx = indices[i];
                    if (idx == -1 || ic.IsNull(idx)) nic.Append(null);
                    else nic.Append(ic.Get(idx));
                }
            }
            else if (source is DoubleColumn dc && newCol is DoubleColumn ndc)
            {
                for (int i = 0; i < indices.Count; i++)
                {
                    int idx = indices[i];
                    if (idx == -1 || dc.IsNull(idx)) ndc.Append(null);
                    else ndc.Append(dc.Get(idx));
                }
            }
            else if (source is StringColumn sc && newCol is StringColumn nsc)
            {
                for (int i = 0; i < indices.Count; i++)
                {
                    int idx = indices[i];
                    if (idx == -1) nsc.Append(null);
                    else nsc.Append(sc.Get(idx));
                }
            }
            else if (source is BoolColumn bc && newCol is BoolColumn nbc)
            {
                for (int i = 0; i < indices.Count; i++)
                {
                    int idx = indices[i];
                    if (idx == -1 || bc.IsNull(idx)) nbc.Append(null);
                    else nbc.Append(bc.Get(idx));
                }
            }
            else if (source is DateTimeColumn dtc && newCol is DateTimeColumn ndtc)
            {
                for (int i = 0; i < indices.Count; i++)
                {
                    int idx = indices[i];
                    if (idx == -1 || dtc.IsNull(idx)) ndtc.Append(null);
                    else ndtc.Append(dtc.Get(idx));
                }
            }
            else
            {
                // Fallback
                for (int i = 0; i < indices.Count; i++)
                {
                    int idx = indices[i];
                    if (idx == -1) newCol.AppendObject(null);
                    else newCol.AppendObject(source.GetValue(idx));
                }
            }

            return newCol;
        }
    }
}