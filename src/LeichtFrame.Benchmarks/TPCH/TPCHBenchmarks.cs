using BenchmarkDotNet.Attributes;
using LeichtFrame.Core;
using LeichtFrame.IO;
using DuckDB.NET.Data;
using static LeichtFrame.Core.Expressions.F;

namespace LeichtFrame.Benchmarks
{
    [MemoryDiagnoser]
    public class TPCHBenchmarks
    {
        private DataFrame _lineItemDf = null!;
        private DuckDBConnection _duckDbConnection = null!;
        private readonly DateTime _targetDate = new DateTime(1998, 9, 2);

        private const string FilePath = "/home/dennis/source/repos/dbgen/tpch-dbgen/lineitem.tbl";

        private const string Q1Sql = @"
            SELECT
                l_returnflag,
                l_linestatus,
                sum(l_quantity) as sum_qty,
                sum(l_extendedprice) as sum_base_price,
                sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
                sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
                avg(l_quantity) as avg_qty,
                avg(l_extendedprice) as avg_price,
                avg(l_discount) as avg_disc,
                count(*) as count_order
            FROM
                lineitem
            WHERE
                l_shipdate <= CAST('1998-09-02' AS DATE)
            GROUP BY
                l_returnflag,
                l_linestatus
            ORDER BY
                l_returnflag,
                l_linestatus;";

        [GlobalSetup]
        public void Setup()
        {
            if (!File.Exists(FilePath))
                throw new FileNotFoundException($"File not found: {FilePath}");

            // 1. LeichtFrame Setup
            var schema = new DataFrameSchema(new[]
            {
                new ColumnDefinition("l_quantity", typeof(double), SourceIndex: 4),
                new ColumnDefinition("l_extendedprice", typeof(double), SourceIndex: 5),
                new ColumnDefinition("l_discount", typeof(double), SourceIndex: 6),
                new ColumnDefinition("l_tax", typeof(double), SourceIndex: 7),
                new ColumnDefinition("l_returnflag", typeof(string), SourceIndex: 8),
                new ColumnDefinition("l_linestatus", typeof(string), SourceIndex: 9),
                new ColumnDefinition("l_shipdate", typeof(DateTime), SourceIndex: 10)
            });

            _lineItemDf = CsvReader.Read(FilePath, schema, new CsvReadOptions
            {
                Separator = "|",
                HasHeader = false,
                HasTrailingDelimiter = true
            });

            // 2. DuckDB Setup
            _duckDbConnection = new DuckDBConnection("DataSource=:memory:");
            _duckDbConnection.Open();

            using var cmd = _duckDbConnection.CreateCommand();
            cmd.CommandText = $@"
                CREATE TABLE lineitem AS 
                SELECT * FROM read_csv('{FilePath}', 
                    header=false, 
                    sep='|', 
                    columns={{
                        'l_orderkey': 'INT', 
                        'l_partkey': 'INT', 
                        'l_suppkey': 'INT', 
                        'l_linenumber': 'INT',
                        'l_quantity': 'DOUBLE', 
                        'l_extendedprice': 'DOUBLE', 
                        'l_discount': 'DOUBLE', 
                        'l_tax': 'DOUBLE',
                        'l_returnflag': 'VARCHAR', 
                        'l_linestatus': 'VARCHAR', 
                        'l_shipdate': 'DATE',
                        'l_commitdate': 'DATE', 
                        'l_receiptdate': 'DATE', 
                        'l_shipinstruct': 'VARCHAR', 
                        'l_shipmode': 'VARCHAR', 
                        'l_comment': 'VARCHAR',
                        'dummy_end': 'VARCHAR' 
                    }});";
            cmd.ExecuteNonQuery();

            Console.WriteLine($"Benchmarks ready. Datasets: {_lineItemDf.RowCount:N0}");
        }

        [Benchmark(Description = "LF Q1: Lazy API")]
        public DataFrame RunQ1LeichtFrame()
        {
            return _lineItemDf.Lazy()
                .Where(Col("l_shipdate") <= _targetDate)
                .Select(
                    Col("l_returnflag"),
                    Col("l_linestatus"),
                    Col("l_quantity"),
                    Col("l_extendedprice"),
                    Col("l_discount"),
                    (Col("l_extendedprice") * (1.0 - Col("l_discount"))).As("disc_price"),
                    (Col("l_extendedprice") * (1.0 - Col("l_discount")) * (1.0 + Col("l_tax"))).As("charge")
                )
                .GroupBy("l_returnflag", "l_linestatus")
                .Agg(
                    Sum(Col("l_quantity")).As("sum_qty"),
                    Sum(Col("l_extendedprice")).As("sum_base_price"),
                    Sum(Col("disc_price")).As("sum_disc_price"),
                    Sum(Col("charge")).As("sum_charge"),
                    Mean(Col("l_quantity")).As("avg_qty"),
                    Mean(Col("l_extendedprice")).As("avg_price"),
                    Mean(Col("l_discount")).As("avg_disc"),
                    Count().As("count_order")
                )
                .OrderBy("l_returnflag", "l_linestatus")
                .Collect();
        }

        [Benchmark(Description = "DuckDB Q1: Pricing Summary (SQL)")]
        public int RunQ1DuckDb()
        {
            using var cmd = _duckDbConnection.CreateCommand();
            cmd.CommandText = Q1Sql;
            using var reader = cmd.ExecuteReader();

            int count = 0;
            while (reader.Read())
            {
                count++;
            }
            return count;
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _lineItemDf?.Dispose();
            _duckDbConnection?.Dispose();
        }
    }
}