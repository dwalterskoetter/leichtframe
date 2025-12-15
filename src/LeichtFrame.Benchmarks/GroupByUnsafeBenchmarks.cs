using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;

namespace LeichtFrame.Benchmarks
{
    public class GroupByUnsafeBenchmarks : BenchmarkData
    {
        // =========================================================
        // SCENARIO 1: LOW CARDINALITY
        // =========================================================

        [Benchmark(Baseline = true, Description = "DuckDB (Low Card)")]
        public long DuckDB_LowCard()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Category, COUNT(*) FROM BenchData GROUP BY Category";
            using var reader = cmd.ExecuteReader();

            long count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LF Standard (Low Card)")]
        public DataFrame LF_Standard_LowCard()
        {
            return _lfFrame.GroupByStandard("Category").Count();
        }

        [Benchmark(Description = "LF Radix (Low Card)")]
        public DataFrame LF_Radix_LowCard()
        {
            return _lfFrame.GroupByRadix("Category").Count();
        }

        // =========================================================
        // SCENARIO 2: HIGH CARDINALITY
        // =========================================================

        [Benchmark(Description = "DuckDB (High Card - Int)")]
        public long DuckDB_HighCard_Int()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Id, COUNT(*) FROM BenchData GROUP BY Id";
            using var reader = cmd.ExecuteReader();

            long count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LF Standard (High Card - Int)")]
        public DataFrame LF_Standard_HighCard_Int()
        {
            return _lfFrame.GroupByStandard("Id").Count();
        }

        [Benchmark(Description = "LF Radix (High Card - Int)")]
        public DataFrame LF_Radix_HighCard_Int()
        {
            return _lfFrame.GroupByRadix("Id").Count();
        }

        // =========================================================
        // SCENARIO 3: HIGH CARDINALITY
        // =========================================================

        [Benchmark(Description = "DuckDB (High Card - String)")]
        public long DuckDB_HighCard_String()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT UniqueId, COUNT(*) FROM BenchData GROUP BY UniqueId";
            using var reader = cmd.ExecuteReader();

            long count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LF Standard (High Card - String)")]
        public DataFrame LF_Standard_HighCard_String()
        {
            return _lfFrame.GroupByStandard("UniqueId").Count();
        }

        [Benchmark(Description = "LF Radix (High Card - String)")]
        public DataFrame LF_Radix_HighCard_String()
        {
            return _lfFrame.GroupByRadix("UniqueId").Count();
        }
    }
}