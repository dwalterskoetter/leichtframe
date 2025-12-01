namespace LeichtFrame.Core.Tests.DataFrameTests
{
    public class JoinTests
    {
        [Fact]
        public void InnerJoin_Integers_Matches_Correctly()
        {
            // Left: Employees
            var left = DataFrame.Create(new DataFrameSchema(new[] {
                new ColumnDefinition("EmpId", typeof(int)),
                new ColumnDefinition("DeptId", typeof(int))
            }), 10);
            ((IntColumn)left["EmpId"]).Append(1); ((IntColumn)left["DeptId"]).Append(100);
            ((IntColumn)left["EmpId"]).Append(2); ((IntColumn)left["DeptId"]).Append(200);
            ((IntColumn)left["EmpId"]).Append(3); ((IntColumn)left["DeptId"]).Append(100);

            // Right: Departments
            var right = DataFrame.Create(new DataFrameSchema(new[] {
                new ColumnDefinition("DeptId", typeof(int)),
                new ColumnDefinition("DeptName", typeof(string))
            }), 10);
            ((IntColumn)right["DeptId"]).Append(100); ((StringColumn)right["DeptName"]).Append("IT");
            ((IntColumn)right["DeptId"]).Append(300); ((StringColumn)right["DeptName"]).Append("HR");

            // Join on DeptId
            var joined = left.Join(right, on: "DeptId", JoinType.Inner);

            // Expect 2 rows (Emp 1 and 3 match Dept 100)
            Assert.Equal(2, joined.RowCount);

            // Check Data
            Assert.Equal("IT", joined["DeptName"].Get<string>(0));
            Assert.Equal(1, joined["EmpId"].Get<int>(0));
            Assert.Equal(3, joined["EmpId"].Get<int>(1));
        }

        [Fact]
        public void InnerJoin_Strings_Matches_Correctly()
        {
            var left = DataFrame.Create(new DataFrameSchema(new[] { new ColumnDefinition("Key", typeof(string)) }), 5);
            ((StringColumn)left["Key"]).Append("A");
            ((StringColumn)left["Key"]).Append("B");

            var right = DataFrame.Create(new DataFrameSchema(new[] {
                new ColumnDefinition("Key", typeof(string)),
                new ColumnDefinition("Val", typeof(int))
            }), 5);
            ((StringColumn)right["Key"]).Append("A"); ((IntColumn)right["Val"]).Append(99);

            var joined = left.Join(right, on: "Key");

            Assert.Equal(1, joined.RowCount);
            Assert.Equal("A", joined["Key"].Get<string>(0));
            Assert.Equal(99, joined["Val"].Get<int>(0));
        }

        [Fact]
        public void InnerJoin_1_to_N_Explodes_Rows()
        {
            // Left: 1 Row with Key 1
            var left = DataFrame.Create(new DataFrameSchema(new[] { new ColumnDefinition("K", typeof(int)) }), 5);
            ((IntColumn)left["K"]).Append(1);

            // Right: 2 Rows with Key 1
            var right = DataFrame.Create(new DataFrameSchema(new[] {
                new ColumnDefinition("K", typeof(int)),
                new ColumnDefinition("V", typeof(string))
            }), 5);

            ((IntColumn)right["K"]).Append(1); ((StringColumn)right["V"]).Append("M1");
            ((IntColumn)right["K"]).Append(1); ((StringColumn)right["V"]).Append("M2");

            var joined = left.Join(right, on: "K");

            Assert.Equal(2, joined.RowCount);
            Assert.Equal("M1", joined["V"].Get<string>(0));
            Assert.Equal("M2", joined["V"].Get<string>(1));
        }
    }
}