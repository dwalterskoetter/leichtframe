using System.Text;
using LeichtFrame.Core;
using LeichtFrame.IO;

Console.WriteLine("=========================================================");
Console.WriteLine("   🚀 LeichtFrame - End-to-End Data Pipeline Demo");
Console.WriteLine("=========================================================");

// ---------------------------------------------------------
// 1. SETUP: Simulate Messy Input Data (CSV)
// ---------------------------------------------------------
// In a real app, this would be a file on disk.
// We have:
// - Missing IDs (null/0)
// - Empty Department names (messy data)
// - Valid numerical data
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
// 2. DEFINE SCHEMA
// ---------------------------------------------------------
// Strict typing is the key to performance.
var schema = new DataFrameSchema(new[] {
    new ColumnDefinition("TransactionId", typeof(int)),
    new ColumnDefinition("Department", typeof(string), IsNullable: true),
    new ColumnDefinition("SalesAmount", typeof(double)),
    new ColumnDefinition("IsRefund", typeof(bool))
});

// ---------------------------------------------------------
// 3. READ CSV (Streaming)
// ---------------------------------------------------------
Console.WriteLine("[2] Reading CSV into DataFrame...");
var df = CsvReader.Read(memoryStream, schema);

Console.WriteLine($"    Loaded {df.RowCount} rows.");
Console.WriteLine("    Raw Data Preview:");
Console.WriteLine(df.Inspect());

// ---------------------------------------------------------
// 4. CLEANING (Filter)
// ---------------------------------------------------------
// Logic: We want to remove rows where 'Department' is empty/null 
// OR where it is a Refund.
Console.WriteLine("[3] Cleaning Data (Removing missing Departments & Refunds)...");

var cleanedDf = df.Where(row =>
{
    // High-performance typed access via RowView
    string dept = row.Get<string>("Department");
    bool isRefund = row.Get<bool>("IsRefund");

    // Keep only if Department exists AND it's not a refund
    return !string.IsNullOrEmpty(dept) && !isRefund;
});

Console.WriteLine($"    Cleaned Data: {cleanedDf.RowCount} rows remaining.");
Console.WriteLine(cleanedDf.Inspect());

// ---------------------------------------------------------
// 5. AGGREGATION (GroupBy & Sum)
// ---------------------------------------------------------
// Logic: Calculate total sales per Department
Console.WriteLine("[4] Aggregating: Total Sales by Department...");

// GroupBy creates a hash-map of indices, then Sum aggregates the specific column
var reportDf = cleanedDf.GroupBy("Department").Sum("SalesAmount");

Console.WriteLine("    Report Result:");
Console.WriteLine(reportDf.Inspect());

// ---------------------------------------------------------
// 6. EXPORT (Parquet)
// ---------------------------------------------------------
// Parquet is columnar and compressed, perfect for Big Data systems.
string outputPath = "sales_report.parquet";
Console.WriteLine($"[5] Exporting Report to Parquet: '{outputPath}'...");

if (File.Exists(outputPath)) File.Delete(outputPath);
reportDf.WriteParquet(outputPath);

Console.WriteLine("✅ Done! Pipeline executed successfully.");
Console.WriteLine("=========================================================");