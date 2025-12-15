using LeichtFrame.Core;

namespace LeichtFrame.Core.Tests.DataFrameTests
{
    public class GroupingTests
    {
        [Fact]
        public void GroupBy_Strings_creates_Correct_Buckets()
        {
            // Arrange
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Dept", typeof(string)),
                new ColumnDefinition("Id", typeof(int))
            });
            var df = DataFrame.Create(schema, 10);

            var dept = (StringColumn)df["Dept"];
            dept.Append("Sales"); // 0
            dept.Append("IT");    // 1
            dept.Append("Sales"); // 2
            dept.Append("HR");    // 3
            dept.Append("IT");    // 4

            // Act
            var grouped = df.GroupBy("Dept");

            // Assert
            Assert.Equal(3, grouped.GroupCount); // Sales, IT, HR

            // Helper to find group index by key
            int salesGroupIdx = FindGroupIndex(grouped, "Sales");
            int itGroupIdx = FindGroupIndex(grouped, "IT");

            Assert.True(salesGroupIdx >= 0, "Sales group not found");
            Assert.True(itGroupIdx >= 0, "IT group not found");

            // Check Sales bucket (Indices 0 and 2)
            var salesIndices = GetIndicesForGroup(grouped, salesGroupIdx);
            Assert.Contains(0, salesIndices);
            Assert.Contains(2, salesIndices);
            Assert.Equal(2, salesIndices.Length);

            // Check IT bucket (Indices 1 and 4)
            var itIndices = GetIndicesForGroup(grouped, itGroupIdx);
            Assert.Contains(1, itIndices);
            Assert.Contains(4, itIndices);
            Assert.Equal(2, itIndices.Length);
        }

        [Fact]
        public void GroupBy_Handles_Nulls_As_Separate_Group()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] {
                new ColumnDefinition("Cat", typeof(string), IsNullable: true)
            }), 5);
            var col = (StringColumn)df["Cat"];

            col.Append("A");    // 0
            col.Append(null);   // 1
            col.Append("A");    // 2
            col.Append(null);   // 3

            var grouped = df.GroupBy("Cat");

            // "A" is one group, Nulls are separate
            Assert.Equal(1, grouped.GroupCount);
            Assert.NotNull(grouped.NullGroupIndices);

            // Check Null Indices
            Assert.Equal(2, grouped.NullGroupIndices!.Length);
            Assert.Contains(1, grouped.NullGroupIndices);
            Assert.Contains(3, grouped.NullGroupIndices);

            // Check "A" Group
            int aIdx = FindGroupIndex(grouped, "A");
            var aIndices = GetIndicesForGroup(grouped, aIdx);
            Assert.Equal(2, aIndices.Length);
            Assert.Contains(0, aIndices);
            Assert.Contains(2, aIndices);
        }

        [Fact]
        public void GroupBy_Integers_Works()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] { new ColumnDefinition("Num", typeof(int)) }), 5);
            var col = (IntColumn)df["Num"];
            col.Append(10);
            col.Append(20);
            col.Append(10);

            var grouped = df.GroupBy("Num");

            Assert.Equal(2, grouped.GroupCount); // 2 Groups overall (10 and 20)

            int g10 = FindGroupIndex(grouped, 10);
            int g20 = FindGroupIndex(grouped, 20);

            Assert.Equal(2, GetIndicesForGroup(grouped, g10).Length);
            Assert.Single(GetIndicesForGroup(grouped, g20));
        }

        [Fact]
        public void Group_Count_Returns_Correct_DataFrame()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] { new ColumnDefinition("Dept", typeof(string)) }), 10);
            var col = (StringColumn)df["Dept"];
            col.Append("IT");
            col.Append("Sales");
            col.Append("IT");

            var result = df.GroupBy("Dept").Count();

            Assert.Equal(2, result.RowCount);

            var itRow = result.Where(r => r.Get<string>("Dept") == "IT");
            Assert.Equal(2, itRow["Count"].Get<int>(0));
        }

        [Fact]
        public void Group_Sum_Calculates_Totals()
        {
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Val", typeof(double))
            });
            var df = DataFrame.Create(schema, 10);

            var id = (IntColumn)df["Id"];
            var val = (DoubleColumn)df["Val"];

            // Group 1: 10 + 20 = 30
            id.Append(1); val.Append(10.0);
            id.Append(1); val.Append(20.0);

            // Group 2: 5 = 5
            id.Append(2); val.Append(5.0);

            var result = df.GroupBy("Id").Sum("Val");

            Assert.Equal(2, result.RowCount);

            // Check Sum for ID 1
            var g1 = result.Where(r => r.Get<int>("Id") == 1);
            Assert.Equal(30.0, g1["Sum_Val"].Get<double>(0));
        }

        [Fact]
        public void Group_Sum_Handles_Null_Values_In_Data()
        {
            var df = DataFrame.Create(new DataFrameSchema(new[] {
                new ColumnDefinition("G", typeof(string)),
                new ColumnDefinition("V", typeof(int), IsNullable: true)
            }), 5);

            var g = (StringColumn)df["G"];
            var v = (IntColumn)df["V"];

            g.Append("A"); v.Append(10);
            g.Append("A"); v.Append(null); // Should be ignored
            g.Append("A"); v.Append(5);

            var result = df.GroupBy("G").Sum("V");

            Assert.Equal(15.0, result["Sum_V"].Get<double>(0));
        }

        [Fact]
        public void GroupBy_Large_Data_Consistency()
        {
            // Test robustness of CSR logic with larger data
            int rowCount = 5000;
            int distinctGroups = 10;

            using var col = new IntColumn("Val", rowCount);
            for (int i = 0; i < rowCount; i++) col.Append(i % distinctGroups);

            var df = new DataFrame(new[] { col });

            var grouped = df.GroupBy("Val");
            var result = grouped.Count();

            Assert.Equal(distinctGroups, result.RowCount);

            // Each group should have exactly (5000 / 10) = 500 items
            int expectedCount = rowCount / distinctGroups;

            var countCol = (IntColumn)result["Count"];
            for (int i = 0; i < result.RowCount; i++)
            {
                Assert.Equal(expectedCount, countCol.Get(i));
            }
        }

        [Fact]
        public void Group_MinMaxMean_Works_Correctly()
        {
            // Arrange
            var schema = new DataFrameSchema(new[] {
                new ColumnDefinition("Group", typeof(string)),
                new ColumnDefinition("Val", typeof(double))
            });
            var df = DataFrame.Create(schema, 10);
            var g = (StringColumn)df["Group"];
            var v = (DoubleColumn)df["Val"];

            // Group A: 10, 20, 30 -> Min: 10, Max: 30, Mean: 20
            g.Append("A"); v.Append(10.0);
            g.Append("A"); v.Append(20.0);
            g.Append("A"); v.Append(30.0);

            // Group B: 5, 5 -> Min: 5, Max: 5, Mean: 5
            g.Append("B"); v.Append(5.0);
            g.Append("B"); v.Append(5.0);

            var gdf = df.GroupBy("Group");

            // Min
            var minDf = gdf.Min("Val");
            var rowA_Min = minDf.Where(r => r.Get<string>("Group") == "A");
            Assert.Equal(10.0, rowA_Min["Min_Val"].Get<double>(0));

            // Max
            var maxDf = gdf.Max("Val");
            var rowA_Max = maxDf.Where(r => r.Get<string>("Group") == "A");
            Assert.Equal(30.0, rowA_Max["Max_Val"].Get<double>(0));

            // Mean
            var meanDf = gdf.Mean("Val");
            var rowA_Mean = meanDf.Where(r => r.Get<string>("Group") == "A");
            Assert.Equal(20.0, rowA_Mean["Mean_Val"].Get<double>(0));
        }

        // --- Helper Methods to inspect internal CSR structure ---

        private int FindGroupIndex(GroupedDataFrame gdf, object key)
        {
            var keys = gdf.GetKeys();
            for (int i = 0; i < keys.Length; i++)
            {
                if (object.Equals(keys.GetValue(i), key)) return i;
            }
            return -1;
        }

        private int[] GetIndicesForGroup(GroupedDataFrame gdf, int groupIdx)
        {
            int start = gdf.GroupOffsets[groupIdx];
            int end = gdf.GroupOffsets[groupIdx + 1];
            int len = end - start;
            int[] result = new int[len];

            // Manual copy from CSR indices array
            for (int i = 0; i < len; i++)
            {
                result[i] = gdf.RowIndices[start + i];
            }
            return result;
        }
    }
}