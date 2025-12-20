using BenchmarkDotNet.Attributes;
using LeichtFrame.Core.Operations.Transform;
using LeichtFrame.Core;

namespace LeichtFrame.Benchmarks
{
    public class CalculationBenchmarks : BenchmarkData
    {
        // =========================================================
        // VECTORIZED ARITHMETIC (SIMD)
        // =========================================================

        [Benchmark(Baseline = true, Description = "DuckDB Vec Add (Col + Col)")]
        public double DuckDB_Vec_Add()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Val + Val FROM BenchData";
            using var reader = cmd.ExecuteReader();

            double sum = 0;
            while (reader.Read())
            {
                sum += reader.GetDouble(0);
            }
            return sum;
        }

        [Benchmark(Description = "LeichtFrame Vec Add (Col + Col)")]
        public DoubleColumn LF_Vec_Add()
        {
            var col = (DoubleColumn)_lfFrame["Val"];
            // Uses SIMD instructions via Vector<T>
            return col + col;
        }

        [Benchmark(Description = "DuckDB Vec Scalar (Col * 1.5)")]
        public double DuckDB_Vec_Scalar()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Val * 1.5 FROM BenchData";
            using var reader = cmd.ExecuteReader();

            double sum = 0;
            while (reader.Read())
            {
                sum += reader.GetDouble(0);
            }
            return sum;
        }

        [Benchmark(Description = "LeichtFrame Vec Scalar (Col * 1.5)")]
        public DoubleColumn LF_Vec_Scalar()
        {
            var col = (DoubleColumn)_lfFrame["Val"];
            return col * 1.5;
        }

        // =========================================================
        // COMPUTED COLUMNS (Transformation)
        // =========================================================

        [Benchmark(Description = "DuckDB Computed (Val * Id)")]
        public void DuckDB_Computed()
        {
            using var cmd = _duckConnection.CreateCommand();
            // SQL handles mixed types (Double * Int) automatically
            cmd.CommandText = "SELECT Val * Id FROM BenchData";
            using var reader = cmd.ExecuteReader();

            while (reader.Read()) { }
        }

        [Benchmark(Description = "LeichtFrame AddColumn (Val * Id)")]
        public DataFrame LF_Computed()
        {
            // Creates a new column using a row-based delegate
            return _lfFrame.AddColumn("Computed", row =>
                row.Get<double>("Val") * row.Get<int>("Id")
            );
        }

        // =========================================================
        // STRING TRANSFORMATION
        // =========================================================

        [Benchmark(Description = "DuckDB String Concat")]
        public void DuckDB_String_Concat()
        {
            using var cmd = _duckConnection.CreateCommand();
            // SQL Concat operator
            cmd.CommandText = "SELECT Category || '_' || UniqueId FROM BenchData";
            using var reader = cmd.ExecuteReader();

            while (reader.Read()) { }
        }

        [Benchmark(Description = "LeichtFrame String Concat")]
        public DataFrame LF_String_Concat()
        {
            // This tests the overhead of string allocation per row
            return _lfFrame.AddColumn("Concat", row =>
                row.Get<string>("Category") + "_" + row.Get<string>("UniqueId")
            );
        }
    }
}