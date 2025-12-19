using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.CsProj;

namespace LeichtFrame.Benchmarks
{
    public class Program
    {
        public static void Main(string[] args)
        {
            // --- 1. Custom Arguments Parsing ---           
            bool isFastMode = args.Any(a => a.Equals("fast", StringComparison.OrdinalIgnoreCase) ||
                                            a.Equals("short", StringComparison.OrdinalIgnoreCase));

            // Clean BDN Arguments
            var bdnArgs = args.Where(a => !a.Equals("fast", StringComparison.OrdinalIgnoreCase) &&
                                          !a.Equals("short", StringComparison.OrdinalIgnoreCase)).ToList();

            // "Magic" Argument: "all" -> "--filter *"
            if (bdnArgs.Count > 0 && bdnArgs[0].Equals("all", StringComparison.OrdinalIgnoreCase))
            {
                bdnArgs.Clear();
                bdnArgs.Add("--filter");
                bdnArgs.Add("*");
            }

            // --- 2. Job Configuration ---
            var job = Job.Default
                .WithRuntime(CoreRuntime.Core80)
                .WithPlatform(Platform.X64)
                .WithToolchain(CsProjCoreToolchain.NetCoreApp80);

            if (isFastMode)
            {
                job = job
                    .WithWarmupCount(1)
                    .WithIterationCount(3)
                    .WithLaunchCount(1)
                    .WithInvocationCount(16);
            }
            else
            {
                // STABLE MODE
                job = job
                    .WithLaunchCount(3)
                    .WithWarmupCount(4)
                    .WithIterationCount(10);
            }

            var config = ManualConfig.Create(DefaultConfig.Instance)
                .AddJob(job)
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

            Console.WriteLine($"Mode:    {(isFastMode ? "⚡ FAST / DEV (Low Precision)" : "🛡️  STABLE (High Precision)")}");
            Console.WriteLine("Target:  Comparison against DuckDB.NET");
            Console.WriteLine("Dataset: 1,000,000 Rows (configured via Params)");
            Console.WriteLine();

            // --- 4. Help-Text ---
            if (bdnArgs.Count == 0)
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Usage Examples:");
                Console.WriteLine("  1. Interactive Menu:   dotnet run -c Release");
                Console.WriteLine("  2. Run All (Stable):   dotnet run -c Release -- all");
                Console.WriteLine("  3. Run All (Fast):     dotnet run -c Release -- all fast");
                Console.WriteLine("  4. Filter (Fast):      dotnet run -c Release -- fast --filter \"*GroupBy*\"");
                Console.ResetColor();
                Console.WriteLine();
                Console.WriteLine("Select benchmarks from the list below:");
            }

            // --- 5. Start ---
            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(bdnArgs.ToArray(), config);
        }
    }
}