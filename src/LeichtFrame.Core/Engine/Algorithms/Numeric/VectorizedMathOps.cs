using System.Numerics;
using System.Runtime.InteropServices;

namespace LeichtFrame.Core.Engine
{
    internal static class VectorizedMathOps
    {
        public enum MathOp { Add, Subtract, Multiply, Divide }

        public static void Calculate<T>(ReadOnlySpan<T> a, ReadOnlySpan<T> b, Span<T> result, MathOp op)
            where T : struct, INumber<T>
        {
            int i = 0;
            if (Vector.IsHardwareAccelerated)
            {
                int vecSize = Vector<T>.Count;
                var vecA = MemoryMarshal.Cast<T, Vector<T>>(a);
                var vecB = MemoryMarshal.Cast<T, Vector<T>>(b);
                var vecRes = MemoryMarshal.Cast<T, Vector<T>>(result);

                int limit = Math.Min(vecA.Length, vecB.Length);

                for (int v = 0; v < limit; v++)
                {
                    vecRes[v] = op switch
                    {
                        MathOp.Add => vecA[v] + vecB[v],
                        MathOp.Subtract => vecA[v] - vecB[v],
                        MathOp.Multiply => vecA[v] * vecB[v],
                        MathOp.Divide => vecA[v] / vecB[v],
                        _ => throw new NotSupportedException()
                    };
                }
                i = limit * vecSize;
            }

            // Tail Loop
            for (; i < a.Length; i++)
            {
                result[i] = op switch
                {
                    MathOp.Add => a[i] + b[i],
                    MathOp.Subtract => a[i] - b[i],
                    MathOp.Multiply => a[i] * b[i],
                    MathOp.Divide => a[i] / b[i],
                    _ => throw new NotSupportedException()
                };
            }
        }

        // Overload für Scalar (Column + 5)
        public static void CalculateScalar<T>(ReadOnlySpan<T> a, T scalar, Span<T> result, MathOp op)
            where T : struct, INumber<T>
        {
            int i = 0;
            if (Vector.IsHardwareAccelerated)
            {
                int vecSize = Vector<T>.Count;
                var vecA = MemoryMarshal.Cast<T, Vector<T>>(a);
                var vecRes = MemoryMarshal.Cast<T, Vector<T>>(result);
                var vecScalar = new Vector<T>(scalar);

                int limit = vecA.Length;

                for (int v = 0; v < limit; v++)
                {
                    vecRes[v] = op switch
                    {
                        MathOp.Add => vecA[v] + vecScalar,
                        MathOp.Subtract => vecA[v] - vecScalar,
                        MathOp.Multiply => vecA[v] * vecScalar,
                        MathOp.Divide => vecA[v] / vecScalar,
                        _ => throw new NotSupportedException()
                    };
                }
                i = limit * vecSize;
            }

            for (; i < a.Length; i++)
            {
                result[i] = op switch
                {
                    MathOp.Add => a[i] + scalar,
                    MathOp.Subtract => a[i] - scalar,
                    MathOp.Multiply => a[i] * scalar,
                    MathOp.Divide => a[i] / scalar,
                    _ => throw new NotSupportedException()
                };
            }
        }
    }
}