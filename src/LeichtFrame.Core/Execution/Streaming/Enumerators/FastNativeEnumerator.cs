using System.Collections;
using LeichtFrame.Core.Engine;
using LeichtFrame.Core.Execution.Streaming.Columns;
using LeichtFrame.Core.Expressions;

namespace LeichtFrame.Core.Execution.Streaming.Enumerators
{
    internal unsafe class FastNativeEnumerator : IEnumerator<RowView>
    {
        private readonly GroupedDataFrame _gdf;
        private readonly NativeGroupedData _native;
        private readonly int* _pKeys;
        private readonly int* _pOffsets;
        private readonly int* _pIndices;
        private readonly int _groupCount;

        private int _currentIndex = -1;
        private bool _inNullGroup = false;

        private readonly AggOpType _op;
        private readonly IColumn? _valueCol;
        private readonly IFlyweightKeyColumn[] _keyCols;
        private readonly ScalarResultColumn _resCol;
        private readonly RowView _currentView;

        private readonly int _startIdx;

        public FastNativeEnumerator(GroupedDataFrame gdf, string[] keyNames, string resName, AggOpType op, IColumn? valueCol)
        {
            _gdf = gdf;
            _native = gdf.NativeData!;
            _op = op;
            _valueCol = valueCol;

            _startIdx = 0;
            DictionaryGroupedDataFrame? dictGdf = gdf as DictionaryGroupedDataFrame;
            if (dictGdf != null) _startIdx = dictGdf.NativeStartOffset;

            _pKeys = _native.Keys.Ptr + _startIdx;
            _pOffsets = _native.Offsets.Ptr + _startIdx;
            _pIndices = _native.Indices.Ptr;

            _groupCount = _native.GroupCount - _startIdx;

            Type resType = op == AggOpType.Count ? typeof(int) : typeof(double);
            _resCol = new ScalarResultColumn(resName, resType);

            _keyCols = new IFlyweightKeyColumn[keyNames.Length];
            var viewColumns = new IColumn[keyNames.Length + 1];
            var schemaCols = new List<ColumnDefinition>();

            for (int i = 0; i < keyNames.Length; i++)
            {
                string name = keyNames[i];
                var sourceCol = gdf.Source[name];

                if (gdf.KeysAreRowIndices)
                {
                    _keyCols[i] = CreateIndirectColumn(sourceCol, name);
                }
                else if (dictGdf != null && i == 0)
                {
                    var dict = dictGdf.InternalDictionary;
                    _keyCols[i] = new DictionaryKeyColumn(name, dict);
                }
                else
                {
                    if (sourceCol.DataType != typeof(int))
                        throw new InvalidOperationException($"Direct Native Keys only supported for Int. Got {sourceCol.DataType.Name}");
                    _keyCols[i] = new ScalarKeyColumn<int>(name);
                }

                viewColumns[i] = (IColumn)_keyCols[i];
                schemaCols.Add(new ColumnDefinition(name, _keyCols[i].DataType, IsNullable: true));
            }

            viewColumns[keyNames.Length] = _resCol;
            schemaCols.Add(new ColumnDefinition(resName, resType));

            var schema = new DataFrameSchema(schemaCols);
            _currentView = new RowView(0, viewColumns, schema);
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
            if (_currentIndex < _groupCount - 1)
            {
                _currentIndex++;

                int keyOrIndex = _pKeys[_currentIndex];
                for (int i = 0; i < _keyCols.Length; i++) _keyCols[i].SetData(keyOrIndex, isNull: false);

                int start = _pOffsets[_currentIndex];
                int end = _pOffsets[_currentIndex + 1];

                if (_op == AggOpType.Count) _resCol.SetInt(end - start);
                else if (_op == AggOpType.Sum) _resCol.SetDouble(ComputeSum(start, end));

                return true;
            }

            if (!_inNullGroup)
            {
                bool hasNativeNull = _startIdx == 1;
                bool hasManagedNull = _gdf.NullGroupIndices != null && _gdf.NullGroupIndices.Length > 0;
                bool useManaged = hasManagedNull;
                bool useNative = hasNativeNull && !useManaged;

                if (useManaged || useNative)
                {
                    _inNullGroup = true;
                    for (int i = 0; i < _keyCols.Length; i++) _keyCols[i].SetData(0, isNull: true);

                    if (_op == AggOpType.Count)
                    {
                        int count = 0;
                        if (useManaged) count = _gdf.NullGroupIndices!.Length;
                        else if (useNative) unsafe { count = _native.Offsets.Ptr[1] - _native.Offsets.Ptr[0]; }
                        _resCol.SetInt(count);
                    }
                    else if (_op == AggOpType.Sum)
                    {
                        double sum = 0;
                        if (useManaged)
                        {
                            if (_valueCol is DoubleColumn dc) { var s = dc.Values.Span; foreach (var x in _gdf.NullGroupIndices!) sum += s[x]; }
                            else if (_valueCol is IntColumn ic) { var s = ic.Values.Span; foreach (var x in _gdf.NullGroupIndices!) sum += s[x]; }
                        }
                        else if (useNative)
                        {
                            unsafe { sum += ComputeSum(_native.Offsets.Ptr[0], _native.Offsets.Ptr[1]); }
                        }
                        _resCol.SetDouble(sum);
                    }
                    return true;
                }
            }
            return false;
        }

        private double ComputeSum(int start, int end)
        {
            double sum = 0;
            if (_valueCol is DoubleColumn dc)
            {
                var span = dc.Values.Span;
                for (int k = start; k < end; k++) sum += span[_pIndices[k]];
            }
            else if (_valueCol is IntColumn ic)
            {
                var span = ic.Values.Span;
                for (int k = start; k < end; k++) sum += span[_pIndices[k]];
            }
            return sum;
        }

        public void Reset() => throw new NotSupportedException();
        public void Dispose() { _gdf.Dispose(); }
    }
}