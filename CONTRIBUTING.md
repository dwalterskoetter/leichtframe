# Contributing to LeichtFrame

Thank you for your interest in contributing to LeichtFrame! We welcome contributions from everyone, whether it's fixing a bug, improving documentation, or optimizing performance.

## 🛠️ Prerequisites

To build and run this project, you need:

- [.NET 8.0 SDK](https://dotnet.microsoft.com/download/dotnet/8.0) or newer.
- A C# IDE (Visual Studio 2022, JetBrains Rider, or VS Code).

## 🚀 Getting Started

1.  **Fork** the repository on GitHub.
2.  **Clone** your fork locally:
    ```bash
    git clone https://github.com/YOUR_USERNAME/LeichtFrame.git
    cd LeichtFrame
    ```
3.  Create a **feature branch** for your work:
    ```bash
    git checkout -b feature/my-new-optimization
    ```

## 🏗️ Building and Testing

The project follows a standard .NET solution structure.

### Build

To build the entire solution (Core, IO, Benchmarks, Tests):

```bash
dotnet build -c Release
```

### Run Tests

Please ensure all unit tests pass before submitting a PR. We use xUnit.

```bash
dotnet test -c Release
```

## 📊 Performance & Benchmarks

**Performance is the #1 feature of LeichtFrame.**
If you modify the Core engine or IO adapters, you **must** run the benchmarks to ensure no performance regressions occurred.

1.  Navigate to the benchmark directory:
    ```bash
    cd src/LeichtFrame.Benchmarks
    ```
2.  Run the benchmarks (this may take 5-10 minutes):
    ```bash
    dotnet run -c Release --filter "*"
    ```

Compare the results with the baseline in `README.md`. If your change makes the code slower or increases memory allocation, please explain why in your Pull Request.

For detailed profiling instructions (CPU, Memory Allocation), please refer to [docs/PROFILING.md](docs/PROFILING.md).

## 📝 Coding Standards

- **Zero-Allocation:** This is critical. Avoid LINQ (`.Select()`, `.Where()`, `.ToList()`) inside hot loops. Use `Span<T>`, `ReadOnlySpan<T>`, and `for`-loops instead.
- **Style:** Follow standard C# coding conventions (PascalCase for public members, camelCase for local variables).
- **Documentation:** All public APIs must have XML documentation comments (`///`).

## 🤝 Pull Request Process

1.  Push your feature branch to your GitHub fork.
2.  Open a Pull Request against the `main` branch of the official repository.
3.  Describe your changes clearly. If it's a performance optimization, include the benchmark results.
4.  Ensure the CI pipeline (GitHub Actions) passes.

Thank you for helping make .NET data processing faster! 🚀
