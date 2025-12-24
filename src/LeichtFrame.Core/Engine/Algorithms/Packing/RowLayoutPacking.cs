using System.Runtime.InteropServices;

namespace LeichtFrame.Core.Engine.Algorithms.Packing
{
    internal static unsafe class RowLayoutPacking
    {
        /// <summary>
        /// Packs multiple columns into a contiguous byte buffer (Row Layout).
        /// Format per Column: [NullFlag (1 Byte)] [Data (N Bytes)]
        /// Uses Parallel.For to saturate memory bandwidth.
        /// </summary>
        /// <returns>Tuple of (BufferPtr, RowWidth)</returns>
        public static (IntPtr Buffer, int Width) Pack(DataFrame df, string[] cols)
        {
            int rowCount = df.RowCount;
            int width = 0;
            var types = new Type[cols.Length];
            var columnData = new IColumn[cols.Length];

            // 1. Calculate Schema Width & Metadata
            for (int i = 0; i < cols.Length; i++)
            {
                var col = df[cols[i]];
                columnData[i] = col;
                types[i] = Nullable.GetUnderlyingType(col.DataType) ?? col.DataType;

                int dataSize = GetSize(types[i]);
                if (dataSize == 0)
                    throw new NotSupportedException($"Packing not supported for type {types[i].Name}");

                width += dataSize + 1;
            }

            byte* buffer = (byte*)NativeMemory.Alloc((nuint)(rowCount * width));

            // 2. Parallel Pack per Column
            int currentOffset = 0;
            for (int c = 0; c < cols.Length; c++)
            {
                var col = columnData[c];
                int offset = currentOffset;
                int stride = width;

                PackColumnParallel(col, buffer, rowCount, stride, offset);

                int size = GetSize(types[c]);
                currentOffset += size + 1;
            }

            return ((IntPtr)buffer, width);
        }

        private static void PackColumnParallel(IColumn col, byte* buffer, int rows, int stride, int offset)
        {
            // --- INT ---
            if (col is IntColumn ic)
            {
                Parallel.For(0, rows, i =>
                {
                    byte* ptr = buffer + (i * stride) + offset;
                    if (ic.IsNull(i))
                    {
                        *ptr = 1;
                        *(int*)(ptr + 1) = 0;
                    }
                    else
                    {
                        *ptr = 0;
                        *(int*)(ptr + 1) = ic.Get(i);
                    }
                });
            }
            // --- DOUBLE ---
            else if (col is DoubleColumn dc)
            {
                Parallel.For(0, rows, i =>
                {
                    byte* ptr = buffer + (i * stride) + offset;
                    if (dc.IsNull(i))
                    {
                        *ptr = 1;
                        *(double*)(ptr + 1) = 0;
                    }
                    else
                    {
                        *ptr = 0;
                        *(double*)(ptr + 1) = dc.Get(i);
                    }
                });
            }
            // --- BOOL ---
            else if (col is BoolColumn bc)
            {
                Parallel.For(0, rows, i =>
                {
                    byte* ptr = buffer + (i * stride) + offset;
                    if (bc.IsNull(i))
                    {
                        *ptr = 1;
                        *(byte*)(ptr + 1) = 0;
                    }
                    else
                    {
                        *ptr = 0;
                        *(byte*)(ptr + 1) = bc.Get(i) ? (byte)1 : (byte)0;
                    }
                });
            }
            // --- DATE TIME ---
            else if (col is DateTimeColumn dtc)
            {
                Parallel.For(0, rows, i =>
                {
                    byte* ptr = buffer + (i * stride) + offset;
                    if (dtc.IsNull(i))
                    {
                        *ptr = 1;
                        *(long*)(ptr + 1) = 0;
                    }
                    else
                    {
                        *ptr = 0;
                        *(long*)(ptr + 1) = dtc.Get(i).Ticks;
                    }
                });
            }
            // --- LONG (Generic Fallback / Future LongColumn) ---
            else if (col is IColumn<long> lc)
            {
                Parallel.For(0, rows, i =>
                {
                    byte* ptr = buffer + (i * stride) + offset;
                    if (lc.IsNull(i))
                    {
                        *ptr = 1;
                        *(long*)(ptr + 1) = 0;
                    }
                    else
                    {
                        *ptr = 0;
                        *(long*)(ptr + 1) = lc.GetValue(i);
                    }
                });
            }
            else
            {
                throw new NotSupportedException($"PackColumn not implemented for {col.GetType().Name}");
            }
        }

        private static int GetSize(Type t)
        {
            if (t == typeof(int)) return 4;
            if (t == typeof(double)) return 8;
            if (t == typeof(bool)) return 1;
            if (t == typeof(long)) return 8;
            if (t == typeof(DateTime)) return 8;
            return 0;
        }
    }
}