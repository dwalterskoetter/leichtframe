using System.Runtime.InteropServices;
using LeichtFrame.Core.Engine;

namespace LeichtFrame.Core
{
    internal class DictionaryGroupedDataFrame : GroupedDataFrame
    {
        private readonly string?[] _dictionary;
        private readonly bool _hasNullCodeZero;
        private string[]? _resolvedKeys;
        private int[]? _nullIndices;
        private int[]? _offsets;
        private int[]? _indices;

        public DictionaryGroupedDataFrame(
            DataFrame df,
            string[] colNames,
            NativeGroupedData nativeData,
            string?[] dictionary,
            bool hasNullCodeZero)
            : base(df, colNames, nativeData)
        {
            _dictionary = dictionary;
            _hasNullCodeZero = hasNullCodeZero;
        }

        internal string?[] InternalDictionary => _dictionary;

        internal int NativeStartOffset
        {
            get
            {
                if (!_hasNullCodeZero || NativeData == null || NativeData.GroupCount == 0) return 0;
                unsafe
                {
                    return NativeData.Keys.Ptr[0] == 0 ? 1 : 0;
                }
            }
        }

        private unsafe void EnsureParsed()
        {
            if (_resolvedKeys != null) return;
            if (NativeData == null) throw new ObjectDisposedException(nameof(DictionaryGroupedDataFrame));

            int rawCount = NativeData.GroupCount;
            int startGroupIdx = NativeStartOffset;
            int validGroupCount = rawCount - startGroupIdx;

            // 1. Extract Null Indices (if not already lazy loaded)
            if (startGroupIdx == 1 && _nullIndices == null)
            {
                int* pOffsets = NativeData.Offsets.Ptr;
                int* pIndices = NativeData.Indices.Ptr;
                int start = pOffsets[0];
                int end = pOffsets[1];
                int len = end - start;
                _nullIndices = new int[len];
                Marshal.Copy((nint)pIndices + (start * 4), _nullIndices, 0, len);
            }

            // 2. Resolve Keys
            int* pRawKeys = NativeData.Keys.Ptr;
            _resolvedKeys = new string[validGroupCount];
            for (int i = 0; i < validGroupCount; i++)
            {
                int code = pRawKeys[i + startGroupIdx];
                _resolvedKeys[i] = _dictionary[code]!;
            }

            // 3. Shift Offsets
            int* pRawOffsets = NativeData.Offsets.Ptr;
            _offsets = new int[validGroupCount + 1];
            int shift = (startGroupIdx == 1) ? pRawOffsets[1] : 0;

            for (int i = 0; i <= validGroupCount; i++)
            {
                _offsets[i] = pRawOffsets[i + startGroupIdx] - shift;
            }

            // 4. Copy Indices (Heavy Allocation)
            int totalIndices = pRawOffsets[rawCount] - shift;
            _indices = new int[totalIndices];
            Marshal.Copy((nint)NativeData.Indices.Ptr + (shift * 4), _indices, 0, totalIndices);
        }

        public override int GroupCount
        {
            get
            {
                if (NativeData != null) return NativeData.GroupCount - NativeStartOffset;
                EnsureParsed();
                return _resolvedKeys!.Length;
            }
        }

        public override Array GetKeys() { EnsureParsed(); return _resolvedKeys!; }
        public override int[] GroupOffsets { get { EnsureParsed(); return _offsets!; } }
        public override int[] RowIndices { get { EnsureParsed(); return _indices!; } }
        public override int[]? NullGroupIndices
        {
            get
            {
                if (_nullIndices != null) return _nullIndices;

                if (NativeData != null && NativeStartOffset == 1)
                {
                    unsafe
                    {
                        int* pOffsets = NativeData.Offsets.Ptr;
                        int start = pOffsets[0];
                        int end = pOffsets[1];
                        int len = end - start;
                        if (len > 0)
                        {
                            _nullIndices = new int[len];
                            Marshal.Copy((nint)NativeData.Indices.Ptr + (start * 4), _nullIndices, 0, len);
                            return _nullIndices;
                        }
                    }
                }

                if (NativeData != null) return null;

                EnsureParsed();
                return _nullIndices;
            }
        }

        public override void Dispose()
        {
            base.Dispose();
        }
    }
}