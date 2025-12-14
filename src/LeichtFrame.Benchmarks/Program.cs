using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.CsProj;
using System;
using System.Linq;

namespace LeichtFrame.Benchmarks
{
    public class Program
    {
        public static void Main(string[] args)
        {
            // --- 1. "Magic" Argument Logic ---
            if (args.Length > 0 && args[0].Equals("all", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine("🟢 Shortcut detected: Running ALL benchmarks...");
                args = new[] { "--filter", "*" };
            }

            // --- 2. Configuration ---
            var config = ManualConfig.Create(DefaultConfig.Instance)
                .AddJob(Job.Default
                    .WithRuntime(CoreRuntime.Core80)
                    .WithPlatform(Platform.X64)
                    .WithToolchain(CsProjCoreToolchain.NetCoreApp80)
                )
                .AddExporter(MarkdownExporter.GitHub)
                .WithOptions(ConfigOptions.JoinSummary)
                .AddLogger(ConsoleLogger.Default)
                .AddColumnProvider(DefaultColumnProviders.Instance);

            // --- 3. Header ---
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("=================================================");
            Console.WriteLine("   🚀 LeichtFrame Benchmark Suite");
            Console.WriteLine("=================================================");
            Console.ResetColor();

            Console.WriteLine("Target:  Comparison against DuckDB.NET (In-Memory)");
            Console.WriteLine("Dataset: 1,000,000 Rows (Synthetic POCOs)");
            Console.WriteLine("Mode:    High-Precision (CsProj Toolchain / x64 / .NET 8)");
            Console.WriteLine();

            // --- 4. Help-Text ---
            if (args.Length == 0)
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Usage Modes:");
                Console.WriteLine("  1. Interactive:  Just press Enter (Menu)");
                Console.WriteLine("  2. Run All:      dotnet run -c Release -- all");
                Console.WriteLine("  3. Filter:       dotnet run -c Release -- --filter *Join*");
                Console.ResetColor();
                Console.WriteLine();
                Console.WriteLine("Select benchmarks from the list below:");
            }

            // --- 5. Start ---
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
        }
    }
}