using System.Collections;
using LeichtFrame.Core.Plans;
using LeichtFrame.Core.Operations.GroupBy;
using LeichtFrame.Core.Operations.Aggregate;
using LeichtFrame.Core.Expressions;
using LeichtFrame.Core.Engine;

namespace LeichtFrame.Core.Execution
{
    /// <summary>
    /// Executes a Logical Plan in a streaming fashion to minimize memory allocation.
    /// </summary>
    public static class PhysicalStreamer
    {
        /// <summary>
        /// Execute Logical Plan
        /// </summary>
        /// <param name="plan"></param>
        /// <returns></returns>
        public static IEnumerable<RowView> Execute(LogicalPlan plan)
        {
            if (plan is Aggregate aggNode)
            {
                return new AggregateStreamEnumerable(aggNode);
            }

            var df = new PhysicalPlanner().Execute(plan);
            return new DataFrameEnumerable(df);
        }

        private class DataFrameEnumerable : IEnumerable<RowView>
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

        private class AggregateStreamEnumerable : IEnumerable<RowView>
        {
            private readonly Aggregate _node;

            public AggregateStreamEnumerable(Aggregate node)
            {
                _node = node;
            }

            public IEnumerator<RowView> GetEnumerator()
            {
                var inputDf = new PhysicalPlanner().Execute(_node.Input);
                var groupCols = _node.GroupExprs.Cast<ColExpr>().Select(c => c.Name).ToArray();

                var gdf = GroupingOps.GroupBy(inputDf, groupCols);

                bool isSimpleCount = _node.AggExprs.Count == 1
                                     && _node.AggExprs[0] is AliasExpr alias
                                     && alias.Child is AggExpr agg
                                     && agg.Op == AggOpType.Count;

                if (isSimpleCount && gdf.NativeData != null && groupCols.Length == 1)
                {
                    var aliasExpr = (AliasExpr)_node.AggExprs[0];
                    return new FastNativeCountEnumerator(gdf, groupCols[0], aliasExpr.Alias);
                }

                // Fallback / Slow Path
                var planner = new PhysicalPlanner();
                var aggDefs = planner.MapAggregations(_node.AggExprs);
                var materializedResult = gdf.Aggregate(aggDefs);

                gdf.Dispose();

                return new DataFrameEnumerable(materializedResult).GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }

        // --- UNSAFE ENUMERATOR ---
        private unsafe class FastNativeCountEnumerator : IEnumerator<RowView>
        {
            private readonly GroupedDataFrame _gdf;
            private readonly NativeGroupedData _native;
            private readonly int* _pKeys;
            private readonly int* _pOffsets;
            private readonly int _groupCount;

            private int _currentIndex = -1;
            private bool _inNullGroup = false;

            // Flyweight Components
            private readonly IFlyweightKeyColumn _keyCol;
            private readonly ScalarIntColumn _valCol;
            private readonly RowView _currentView;

            public FastNativeCountEnumerator(GroupedDataFrame gdf, string keyName, string countName)
            {
                _gdf = gdf;
                _native = gdf.NativeData!;

                _pKeys = _native.Keys.Ptr;
                _pOffsets = _native.Offsets.Ptr;
                _groupCount = _native.GroupCount;

                _valCol = new ScalarIntColumn(countName);

                // --- KEY COLUMN FACTORY ---
                var sourceCol = gdf.Source[keyName];

                if (gdf.KeysAreRowIndices)
                {
                    // Indirect Mode: Native Key = RowIndex
                    _keyCol = CreateIndirectColumn(sourceCol, keyName);
                }
                else
                {
                    // Direct Mode: Native Key = Value
                    if (sourceCol.DataType != typeof(int))
                        throw new InvalidOperationException("Direct Native Keys only supported for Int.");

                    _keyCol = new ScalarIntColumn(keyName);
                }

                // Schema bauen
                var schema = new DataFrameSchema(new[] {
                    new ColumnDefinition(keyName, _keyCol.DataType, IsNullable: true),
                    new ColumnDefinition(countName, typeof(int))
                });

                _currentView = new RowView(0, new IColumn[] { (IColumn)_keyCol, _valCol }, schema);
            }

            private IFlyweightKeyColumn CreateIndirectColumn(IColumn source, string name)
            {
                if (source is IntColumn ic) return new IndirectScalarColumn<int>(name, ic);
                if (source is DoubleColumn dc) return new IndirectScalarColumn<double>(name, dc);
                if (source is StringColumn sc) return new IndirectScalarColumn<string?>(name, sc);
                if (source is BoolColumn bc) return new IndirectScalarColumn<bool>(name, bc);
                if (source is DateTimeColumn dtc) return new IndirectScalarColumn<DateTime>(name, dtc);

                throw new NotSupportedException($"Streaming not supported for type {source.DataType.Name}");
            }

            public RowView Current => _currentView;
            object IEnumerator.Current => _currentView;

            public bool MoveNext()
            {
                // A. Native Groups
                if (_currentIndex < _groupCount - 1)
                {
                    _currentIndex++;

                    int keyOrIndex = _pKeys[_currentIndex];
                    int count = _pOffsets[_currentIndex + 1] - _pOffsets[_currentIndex];

                    // Update Flyweights
                    _keyCol.SetData(keyOrIndex, isNull: false);

                    _valCol.Value = count;
                    _valCol.IsNullValue = false;

                    return true;
                }

                // B. Null Group
                if (!_inNullGroup && _gdf.NullGroupIndices != null && _gdf.NullGroupIndices.Length > 0)
                {
                    _inNullGroup = true;

                    _keyCol.SetData(0, isNull: true); // Key is Null

                    _valCol.Value = _gdf.NullGroupIndices.Length;
                    _valCol.IsNullValue = false;
                    return true;
                }

                return false;
            }

            public void Reset() => throw new NotSupportedException();

            public void Dispose()
            {
                _gdf.Dispose();
            }
        }

        // --- HELPER CLASSES ---

        private interface IFlyweightKeyColumn : IColumn
        {
            void SetData(int keyOrIndex, bool isNull);
        }

        // 1. Direct Int Column
        private class ScalarIntColumn : IColumn<int>, IFlyweightKeyColumn
        {
            public int Value;
            public bool IsNullValue;

            public string Name { get; }
            public Type DataType => typeof(int);
            public int Length => 1;
            public bool IsNullable => true;

            public ScalarIntColumn(string name) { Name = name; }

            public void SetData(int key, bool isNull)
            {
                Value = key;
                IsNullValue = isNull;
            }

            public int GetValue(int index) => Value;
            object? IColumn.GetValue(int index) => IsNullValue ? null : Value;

            public void SetValue(int index, int value) => Value = value;
            public bool IsNull(int index) => IsNullValue;

            // Stubs
            public ReadOnlySpan<int> AsSpan() => throw new NotSupportedException();
            public ReadOnlyMemory<int> Slice(int start, int length) => throw new NotSupportedException();
            public void Append(int value) => throw new NotSupportedException();
            public void AppendObject(object? value) => throw new NotSupportedException();
            public void SetNull(int index) => throw new NotSupportedException();
            public void EnsureCapacity(int capacity) { }
            public IColumn CloneSubset(IReadOnlyList<int> indices) => throw new NotSupportedException();
            public object? ComputeSum(int[] indices, int start, int end) => null;
            public object? ComputeMean(int[] indices, int start, int end) => null;
            public object? ComputeMin(int[] indices, int start, int end) => null;
            public object? ComputeMax(int[] indices, int start, int end) => null;
        }

        // 2. Indirect Column (Lookup from Source)
        private class IndirectScalarColumn<T> : IColumn<T>, IFlyweightKeyColumn
        {
            private readonly IColumn<T> _source;
            private int _currentRowIndex;
            private bool _isNull;

            public string Name { get; }
            public Type DataType => typeof(T);
            public int Length => 1;
            public bool IsNullable => true;

            public IndirectScalarColumn(string name, IColumn<T> source)
            {
                Name = name;
                _source = source;
            }

            public void SetData(int rowIndex, bool isNull)
            {
                _currentRowIndex = rowIndex;
                _isNull = isNull;
            }

            public T GetValue(int index) => _source.GetValue(_currentRowIndex);

            object? IColumn.GetValue(int index)
            {
                if (_isNull) return null;
                return _source.GetValue(_currentRowIndex);
            }

            public bool IsNull(int index) => _isNull;

            // Stubs
            public void SetValue(int index, T value) => throw new NotSupportedException();
            public ReadOnlySpan<T> AsSpan() => throw new NotSupportedException();
            public ReadOnlyMemory<T> Slice(int start, int length) => throw new NotSupportedException();
            public void Append(T value) => throw new NotSupportedException();
            public void AppendObject(object? value) => throw new NotSupportedException();
            public void SetNull(int index) => throw new NotSupportedException();
            public void EnsureCapacity(int capacity) { }
            public IColumn CloneSubset(IReadOnlyList<int> indices) => throw new NotSupportedException();
            public object? ComputeSum(int[] indices, int start, int end) => null;
            public object? ComputeMean(int[] indices, int start, int end) => null;
            public object? ComputeMin(int[] indices, int start, int end) => null;
            public object? ComputeMax(int[] indices, int start, int end) => null;
        }
    }
}