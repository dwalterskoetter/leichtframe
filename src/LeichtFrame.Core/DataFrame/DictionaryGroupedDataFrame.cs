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
        private int _realGroupCount;

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

            EnsureParsed();
            nativeData.Dispose();
            NativeData = null;
        }

        private unsafe void EnsureParsed()
        {
            if (_resolvedKeys != null) return;

            int rawCount = NativeData!.GroupCount;
            int[] rawKeys = new int[rawCount];
            int[] rawOffsets = new int[rawCount + 1];

            Marshal.Copy((nint)NativeData.Keys.Ptr, rawKeys, 0, rawCount);
            Marshal.Copy((nint)NativeData.Offsets.Ptr, rawOffsets, 0, rawCount + 1);

            bool nullGroupFound = _hasNullCodeZero && rawCount > 0 && rawKeys[0] == 0;
            int startGroupIdx = nullGroupFound ? 1 : 0;
            _realGroupCount = rawCount - startGroupIdx;

            if (nullGroupFound)
            {
                int start = rawOffsets[0];
                int end = rawOffsets[1];
                int len = end - start;
                _nullIndices = new int[len];
                Marshal.Copy((nint)NativeData.Indices.Ptr + (start * 4), _nullIndices, 0, len);
            }

            _resolvedKeys = new string[_realGroupCount];
            for (int i = 0; i < _realGroupCount; i++)
            {
                int code = rawKeys[i + startGroupIdx];
                _resolvedKeys[i] = _dictionary[code]!;
            }

            _offsets = new int[_realGroupCount + 1];
            int shift = nullGroupFound ? rawOffsets[1] : 0;

            for (int i = 0; i <= _realGroupCount; i++)
            {
                _offsets[i] = rawOffsets[i + startGroupIdx] - shift;
            }

            int totalIndices = rawOffsets[rawCount] - shift;
            _indices = new int[totalIndices];
            Marshal.Copy((nint)NativeData.Indices.Ptr + (shift * 4), _indices, 0, totalIndices);
        }

        public override int GroupCount { get { EnsureParsed(); return _realGroupCount; } }
        public override Array GetKeys() { EnsureParsed(); return _resolvedKeys!; }
        public override int[] GroupOffsets { get { EnsureParsed(); return _offsets!; } }
        public override int[] RowIndices { get { EnsureParsed(); return _indices!; } }
        public override int[]? NullGroupIndices { get { EnsureParsed(); return _nullIndices; } }
    }
}