using System.Text;

namespace LeichtFrame.Core.Engine.Algorithms.Converter
{
    internal static unsafe class StringConverter
    {
        /// <summary>
        /// Converts a StringColumn to a CategoryColumn avoiding string allocations for existing values.
        /// Optimized for Low Cardinality.
        /// </summary>
        public static CategoryColumn ToCategory(StringColumn col)
        {
            int rowCount = col.Length;
            int[] codes = new int[rowCount];
            var dictionary = new List<string?> { null };

            int mapSize = 1024;
            int mask = mapSize - 1;
            int* buckets = stackalloc int[mapSize];
            new Span<int>(buckets, mapSize).Fill(-1);

            int* codeToRowIdx = stackalloc int[mapSize];

            fixed (byte* pBytes = col.RawBytes)
            fixed (int* pOffsets = col.Offsets)
            {
                for (int i = 0; i < rowCount; i++)
                {
                    if (col.IsNull(i))
                    {
                        codes[i] = 0;
                        continue;
                    }

                    int start = pOffsets[i];
                    int len = pOffsets[i + 1] - start;
                    byte* pStr = pBytes + start;

                    int hash = -2128831035;
                    for (int k = 0; k < len; k++) hash = (hash ^ pStr[k]) * 16777619;

                    int idx = hash & mask;
                    int foundCode = -1;

                    while (true)
                    {
                        int bucketCode = buckets[idx];

                        if (bucketCode == -1)
                        {
                            foundCode = dictionary.Count;
                            dictionary.Add(Encoding.UTF8.GetString(pStr, len));

                            buckets[idx] = foundCode;
                            codeToRowIdx[foundCode] = i;
                            break;
                        }
                        else
                        {
                            string existing = dictionary[bucketCode]!;
                            if (BytesEqualString(pStr, len, existing))
                            {
                                foundCode = bucketCode;
                                break;
                            }
                        }

                        idx = (idx + 1) & mask;
                    }

                    codes[i] = foundCode;
                }
            }
            return CategoryColumn.CreateFromInternals(col.Name, codes, rowCount, dictionary);
        }

        private static bool BytesEqualString(byte* pBytes, int len, string str)
        {
            if (str.Length != len && Encoding.UTF8.GetByteCount(str) != len) return false;

            var strSpan = str.AsSpan();

            byte* temp = stackalloc byte[len];
            int written = Encoding.UTF8.GetBytes(strSpan, new Span<byte>(temp, len));
            if (written != len) return false;

            for (int i = 0; i < len; i++)
            {
                if (pBytes[i] != temp[i]) return false;
            }
            return true;
        }
    }
}