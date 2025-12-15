# LeichtFrame

![Build Status](https://github.com/dwalterskoetter/leichtFrame/actions/workflows/ci.yml/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![NuGet](https://img.shields.io/nuget/v/LeichtFrame.Core.svg)](https://www.nuget.org/packages/LeichtFrame.Core/)

**LeichtFrame** is a high‑performance, zero‑allocation single-node DataFrame engine for **.NET 8+**, designed for **backend and SaaS workloads**.

Inspired by the brilliance of world-class tools like **DuckDB** and **Polars**, LeichtFrame brings modern columnar analytics **natively to the .NET ecosystem**. It provides C# developers with a familiar, strongly-typed experience without the overhead of P/Invoke, unmanaged dependencies, or complex interop layers.

LeichtFrame focuses on minimal memory usage, strict schema typing, and native integration with modern .NET APIs (`Span<T>`, `Memory<T>`, `System.Text.Json`).

---

## Table of Contents

- [Why LeichtFrame?](#-why-leichtframe)
- [Highlights](#-highlights)
- [Benchmarks](#-benchmarks)
- [Installation](#-installation)
- [Quickstart (Example)](#-quickstart-example)
- [Roadmap](#-roadmap)
- [Contributing](#-contributing)
- [License](#-license)

---

## ⚡ Why LeichtFrame?

- **Zero‑Copy Slicing** — Create views, filters, and projections without allocating new buffers using `WhereView` and `Slice`.
- **Columnar Storage** — Contiguous memory (Structure of Arrays) for SIMD usage & better cache locality.
- **Backend‑Ready** — Built for highly concurrent web APIs, not just Jupyter notebooks.
- **Interoperability** — Native support for **Apache Arrow**, **Parquet**, and efficient CSV streaming.

---

## 📊 Benchmarks

LeichtFrame is designed for high-performance in-memory analytics. Below is a comparison against **DuckDB.NET** (the gold standard for in-process OLAP).

**Environment:** Intel Core i9-13900H, .NET 8.0, Linux
**Dataset:** 1,000,000 Rows (Synthetic Data)

| Category         | Operation                     | LeichtFrame | DuckDB      | Result           | LF Memory Alloc |
| :--------------- | :---------------------------- | :---------- | :---------- | :--------------- | :-------------- |
| **Math**         | **Scalar Math** (`col * 1.5`) | **1.08 ms** | 24.84 ms    | **23.0x Faster** | 8.0 MB          |
| **Math**         | **Vector Add** (`col + col`)  | **1.09 ms** | 26.98 ms    | **24.7x Faster** | 8.0 MB          |
| **Aggregations** | **Sum** (`int` / `double`)    | **0.13 ms** | 0.66 ms     | **5.1x Faster**  | ~0 B            |
| **Aggregations** | **Mean** (`Average`)          | **0.15 ms** | 0.60 ms     | **4.0x Faster**  | ~0 B            |
| **Search**       | **Top-N** (Smallest 10)       | **1.21 ms** | 1.92 ms     | **1.6x Faster**  | ~0 B            |
| **IO**           | **Write CSV**                 | 34.4 ms     | **28.8 ms** | 1.2x Slower      | 40 MB           |
| **IO**           | **Read CSV**                  | 70.9 ms     | **53.0 ms** | 1.3x Slower      | 69 MB           |
| **ETL**          | **Distinct** (Unique)         | 23.7 ms     | **7.6 ms**  | 3.1x Slower      | 61 MB           |
| **Analytics**    | **GroupBy** (Count)           | 13.2 ms     | **2.3 ms**  | 5.7x Slower      | 45 MB           |
| **Joins**        | **Inner Join**                | 268 ms      | **23.0 ms** | 11.7x Slower     | 349 MB          |

### ⚡ Key Takeaways

1.  **Number Crunching:** LeichtFrame is significantly faster than DuckDB for direct calculations and aggregations due to **SIMD vectors** and zero-interop overhead.
2.  **Input/Output:** The optimized CSV/Parquet engine performs nearly on par with native C++ implementations.
3.  **Stability:** Even for complex operations (Sorts/Joins) on 1M rows, memory usage remains stable and predictable with no GC pressure spikes.

---

## 📦 Installation

LeichtFrame is available on NuGet:

```bash
# Core Engine
dotnet add package LeichtFrame.Core

# IO Adapters (CSV, Parquet, Arrow)
dotnet add package LeichtFrame.IO
```

---

## 📘 Quickstart Example (End‑to‑End)

LeichtFrame allows you to define your schema using standard C# classes (POCOs) — similar to `CsvHelper` or `EF Core`.

Read CSV → Filter → Aggregate → Export to Parquet.

```csharp
using System;
using LeichtFrame.Core;
using LeichtFrame.IO;

// 1. Define your data structure
public class SalesRecord
{
    public string Department { get; set; }
    public double Sales { get; set; }
    public bool IsActive { get; set; }
}

class Example
{
    static void Main()
    {
        // 2. Read CSV (Schema is inferred from the class 🚀)
        using var df = CsvReader.Read<SalesRecord>("data.csv");

        // 3. High-Performance Filtering (Zero-Allocation view)
        // Uses "WhereView" to avoid copying data
        using var activeSales = df.WhereView(row => row.Get<bool>("IsActive"));

        // 4. Aggregation
        var totalVolume = activeSales.Sum("Sales");
        Console.WriteLine($"Total Sales Volume: {totalVolume}");

        // 5. Export to Parquet (Big Data ready)
        activeSales.WriteParquet("report.parquet");
    }
}
```

---

## 🗺️ Roadmap

### ✅ Completed

- **Core Engine:** Columnar Memory, Typed Schema, Zero-Copy Slicing
- **Math:** SIMD Vectorization (`Vector<T>`) for Aggregations and Arithmetic
- **IO:** Multi-threaded CSV Reader, Parquet Support, Apache Arrow Integration
- **Relational:** Sort (Int/String), GroupBy (Parallel/Sequential), Inner & Left Joins
- **Optimization:** Arrow-style String Storage (Byte-level comparison)

### 🚧 Planned / In Progress

- **Query Optimizer:** Lazy evaluation API (`df.Lazy()`) with predicate pushdown and column pruning.
- **Advanced Analytics:** Window functions (Rank, Lead/Lag) and rolling aggregations.
- **Streaming:** Async Batch Processing for datasets larger than RAM.
- **Connectors:** JSON Streaming Reader/Writer.

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

```

```
