using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;
using static LeichtFrame.Core.Expressions.F; // Importiert Count(), Col()

namespace LeichtFrame.Benchmarks
{
    [MemoryDiagnoser]
    public class LazyVsEagerGroupByBenchmarks : BenchmarkData
    {
        // Szenario: GroupBy auf 'Id' (Int).
        // Das ist deine schnellste Operation (nutzt IntDirectMap oder Radix),
        // daher fällt hier jeder Overhead der Lazy-Architektur am stärksten ins Gewicht.

        [Benchmark(Baseline = true, Description = "DuckDB GroupBy")]
        public long DuckDB_Group()
        {
            using var cmd = _duckConnection.CreateCommand();
            cmd.CommandText = "SELECT Id, COUNT(*) FROM BenchData GROUP BY Id";

            // Wir müssen iterieren, um sicherzustellen, dass DuckDB fertig ist
            using var reader = cmd.ExecuteReader();
            long count = 0;
            while (reader.Read()) count++;
            return count;
        }

        [Benchmark(Description = "LF Eager (Direct API)")]
        public DataFrame LF_Eager()
        {
            // Ruft direkt den Dispatcher auf
            // Keine AST-Allokation, kein Visitor
            return _lfFrame.GroupBy("Id").Count();
        }

        [Benchmark(Description = "LF Lazy (Expression API)")]
        public DataFrame LF_Lazy()
        {
            // 1. AST bauen
            // 2. Optimizer (aktuell Pass-Through)
            // 3. PhysicalPlanner (Matcht Expr -> Engine Call)
            // 4. Engine Execution (Identisch zu Eager)

            return _lfFrame.Lazy()
                           .GroupBy("Id", Count().As("Count"))
                           .Collect();
        }
    }
}