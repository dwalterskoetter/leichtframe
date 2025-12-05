# LeichtFrame 🚀

![Build Status](https://github.com/dwalterskoetter/leichtFrame/actions/workflows/ci.yml/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![NuGet](https://img.shields.io/badge/NuGet-0.1.0--alpha-blue.svg)](https://www.nuget.org/packages/LeichtFrame.Core/)

**LeichtFrame** is a high‑performance, zero‑allocation DataFrame engine for **.NET 8+**, designed for **backend and SaaS workloads**.

Unlike typical data‑science tools, LeichtFrame focuses on minimal memory usage, strict schema typing, and native integration with modern .NET APIs (`Span<T>`, `Memory<T>`, `System.Text.Json`).

---

## Table of Contents

- [Why LeichtFrame?](#-why-leichtframe)
- [Highlights](#-highlights)
- [Benchmarks](#-benchmarks)
- [Installation](#-installation)
- [Quickstart (Example)](#-quickstart-example)
- [Roadmap (MVP Status)](#-roadmap-mvp-status)
- [Contributing](#-contributing)
- [License](#-license)

---

## ⚡ Why LeichtFrame?

- **Zero‑Copy Slicing** — Create views, filters, and projections without allocating new buffers.
- **Columnar Storage** — Contiguous memory (Structure of Arrays) for SIMD usage & better cache locality.
- **Backend‑Ready** — Built for highly concurrent web APIs, not Jupyter notebooks.
- **Interoperability** — Native support for **Apache Arrow**, **Parquet**, and efficient CSV streaming.

---

## 📊 Benchmarks

_Scenario: Processing 1,000,000 rows (Integer Columns)._

| Operation             | LeichtFrame 🚀 | LINQ (Standard) | Microsoft.Data.Analysis | Verdict                    |
| :-------------------- | :------------- | :-------------- | :---------------------- | :------------------------- |
| **Sum (Aggregation)** | **555 μs**     | 1,195 μs        | 995 μs                  | **2.1x faster** than LINQ  |
| **Join (Inner)**      | **349 ms**     | 364 ms          | 520 ms                  | **Faster** than LINQ & MDA |
| **Filter (Where)**    | **37 ms**      | 13 ms           | 56 ms                   | **1.5x faster** than MDA   |
| **Memory (Filter)**   | **6.2 MB**     | 8.4 MB          | 9.8 MB                  | **Lowest Allocation**      |

> **Environment:**  
> 💻 **CPU:** Intel Core i9-13900H (13th Gen)  
> 🐧 **OS:** Linux Mint 22.2  
> 🔧 **Runtime:** .NET 8.0.21 (x64 RyuJIT)  
> 📅 **Date:** Dec 2025  
> _Verified via [BenchmarkDotNet](https://github.com/dotnet/BenchmarkDotNet)._

---

## 📦 Installation

LeichtFrame is available on NuGet (Alpha):

```bash
# Core Engine
dotnet add package LeichtFrame.Core

# IO Adapters (CSV, Parquet, Arrow)
dotnet add package LeichtFrame.IO
```

---

## 📘 Quickstart Example (End‑to‑End)

Read CSV → Filter → Aggregate → Export to Parquet.

```csharp
using System;
using System.IO;
using LeichtFrame.Core;
using LeichtFrame.IO;

class Example
{
    static void Main()
    {
        // 1. For Scripts & Exploration: Auto-Detect Schema (Slower, but easy)
        // var df = CsvReader.Read("data.csv");

        // 2. For High-Performance Production Apps: Define Schema
        // (Prevents double-scanning the file and ensures type safety)
        var schema = new DataFrameSchema(new[] {
            new ColumnDefinition("Department", typeof(string)),
            new ColumnDefinition("Sales", typeof(double)),
            new ColumnDefinition("IsActive", typeof(bool))
        });

        using var stream = File.OpenRead("data.csv");

        var df = CsvReader.Read(stream, schema);

        var activeSales = df.Where(row => row.Get<bool>("IsActive"));

        var totalVolume = activeSales.Sum("Sales");

        Console.WriteLine($"Total Sales Volume: {totalVolume}");

        activeSales.WriteParquet("report.parquet");
    }
}
```

---

## 🗺️ Roadmap (MVP Status)

**Core**

- Columnar Memory
- Schema
- Zero‑Copy Slicing

**API**

- Selection & Filtering
- GroupBy (basic functionality)
- Aggregations

**IO**

- CSV / Parquet / Arrow

**Performance**

- SIMD optimizations (Phase 2)
- Multi‑threaded Aggregations (Phase 2)

**SQL**

- Simple SQL parser (Phase 2)

---

## 🤝 Contributing

Contributions are welcome!

Short version:

1. Fork the repository
2. Create a feature branch
3. Run tests: `dotnet test`
4. Open a Pull Request

---

## 📄 License

MIT License — see `LICENSE`.
