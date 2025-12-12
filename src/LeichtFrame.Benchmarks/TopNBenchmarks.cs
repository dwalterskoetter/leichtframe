using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;

namespace LeichtFrame.Benchmarks
{
    public class TopNBenchmarks : BenchmarkData
    {
        // Wir nutzen die 1 Million Zeilen aus der Basisklasse BenchmarkData (Variable: N)
        // Das Setup (GlobalSetup) erzeugt _lfFrame mit einer Spalte "Val" (Double) 
        // und anderen Spalten.

        [Benchmark(Baseline = true, Description = "OrderBy().Head(10)")]
        public DataFrame FullSort_Head()
        {
            // Die alte Methode: Alles sortieren, dann die ersten 10 nehmen
            // "Val" ist eine Double-Spalte in BenchmarkData
            return _lfFrame.OrderBy("Val").Head(10);
        }

        [Benchmark(Description = "Smallest(10)")]
        public DataFrame Optimized_Smallest()
        {
            // Die neue Methode: Optimiert mit PriorityQueue
            return _lfFrame.Smallest(10, "Val");
        }
    }
}