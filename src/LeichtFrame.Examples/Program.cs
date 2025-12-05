using System.Text;
using LeichtFrame.Core;
using LeichtFrame.IO;

Console.WriteLine("=========================================================");
Console.WriteLine("   🚀 LeichtFrame - End-to-End Data Pipeline Demo");
Console.WriteLine("=========================================================");

// ---------------------------------------------------------
// 1. SETUP: Simulate Messy Input Data (CSV)
// ---------------------------------------------------------
string rawCsvData =
@"TransactionId,Department,SalesAmount,IsRefund
1,Sales,50.00,false
2,IT,120.50,false
3,,0.00,false
4,Sales,300.00,true
5,HR,45.00,false
6,Sales,15.50,false
7,,90.00,false
8,IT,200.00,false";

Console.WriteLine("\n[1] Generating simulated CSV data stream...");
using var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(rawCsvData));

// ---------------------------------------------------------
// 2. DEFINE SCHEMA (The "Gold Standard" Way via POCO)
// ---------------------------------------------------------
// Instead of manually building ColumnDefinitions, we simply use a class.
// This ensures type safety and clean code.
// ---------------------------------------------------------
var df = CsvReader.Read<TransactionData>(memoryStream);

Console.WriteLine($"[2] Read CSV into DataFrame. Loaded {df.RowCount} rows.");
Console.WriteLine("    Raw Data Preview:");
Console.WriteLine(df.Inspect());

// ---------------------------------------------------------
// 3. CLEANING (Filter)
// ---------------------------------------------------------
Console.WriteLine("[3] Cleaning Data (Removing missing Departments & Refunds)...");

var cleanedDf = df.Where(row =>
{
    // High-performance access via generic Get<T>
    string dept = row.Get<string>("Department");
    bool isRefund = row.Get<bool>("IsRefund");

    // Keep only if Department exists AND it's not a refund
    return !string.IsNullOrEmpty(dept) && !isRefund;
});

Console.WriteLine($"    Cleaned Data: {cleanedDf.RowCount} rows remaining.");
Console.WriteLine(cleanedDf.Inspect());

// ---------------------------------------------------------
// 4. AGGREGATION (GroupBy & Sum)
// ---------------------------------------------------------
Console.WriteLine("[4] Aggregating: Total Sales by Department...");

var reportDf = cleanedDf.GroupBy("Department").Sum("SalesAmount");

Console.WriteLine("    Report Result:");
Console.WriteLine(reportDf.Inspect());

// ---------------------------------------------------------
// 5. EXPORT (Parquet)
// ---------------------------------------------------------
string outputPath = "sales_report.parquet";
Console.WriteLine($"[5] Exporting Report to Parquet: '{outputPath}'...");

if (File.Exists(outputPath)) File.Delete(outputPath);
reportDf.WriteParquet(outputPath);

Console.WriteLine("✅ Done! Pipeline executed successfully.");
Console.WriteLine("=========================================================");

// ---------------------------------------------------------
// POCO Definition
// ---------------------------------------------------------
public class TransactionData
{
    public int TransactionId { get; set; }

    // Nullable because input data might have missing values (e.g., "3,,0.00")
    public string? Department { get; set; }

    public double SalesAmount { get; set; }
    public bool IsRefund { get; set; }
}