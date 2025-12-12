using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace LeichtFrame.Benchmarks
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var config = ManualConfig.Create(DefaultConfig.Instance)
                .AddJob(Job.Default.WithToolchain(InProcessEmitToolchain.Instance))

                .AddExporter(MarkdownExporter.GitHub)

                .AddLogger(ConsoleLogger.Default)

                .AddColumnProvider(DefaultColumnProviders.Instance);

            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("=================================================");
            Console.WriteLine("   🚀 LeichtFrame Benchmark Suite");
            Console.WriteLine("=================================================");
            Console.ResetColor();
            Console.WriteLine("Target:  Comparison against LINQ & Microsoft.Data.Analysis");
            Console.WriteLine("Dataset: 1,000,000 Rows (High/Low Cardinality scenarios)");
            Console.WriteLine();

            if (args.Length == 0)
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Tip: Use '--filter *Join*' to run specific tests.");
                Console.WriteLine("Running interactive mode...");
                Console.ResetColor();
                Console.WriteLine();
            }

            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
        }
    }
}