using BenchmarkDotNet.Running;

namespace LeichtFrame.Benchmarks
{
    public class Program
    {
        public static void Main(string[] args)
        {
            // Executes all benchmarks in the assembly and provides a menu
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
        }
    }
}
