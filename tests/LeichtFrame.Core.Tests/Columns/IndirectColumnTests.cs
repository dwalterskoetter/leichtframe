using LeichtFrame.Core;

namespace LeichtFrame.Core.Tests.Columns
{
    public class IndirectColumnTests
    {
        [Fact]
        public void Get_Returns_Correct_Value_Via_Indirection()
        {
            // Source: [10, 20, 30, 40, 50]
            using var source = new IntColumn("Src", 5);
            source.Append(10); source.Append(20); source.Append(30); source.Append(40); source.Append(50);

            // View: Only indices 1 (20) and 3 (40)
            var indices = new[] { 1, 3 };
            using var view = new IndirectColumn<int>(source, indices);

            Assert.Equal(2, view.Length);
            Assert.Equal(20, view.Get(0));
            Assert.Equal(40, view.Get(1));
        }

        [Fact]
        public void SetValue_Writes_Through_To_Source()
        {
            using var source = new IntColumn("Src", 3);
            source.Append(100);
            source.Append(200);
            source.Append(300);

            // View on index 1 (200)
            var view = new IndirectColumn<int>(source, new[] { 1 });

            // Act: Change value in View
            view.SetValue(0, 999);

            // Assert: Source must be updated
            Assert.Equal(999, source.Get(1));
        }

        [Fact]
        public void CloneSubset_Materializes_View_To_Real_Column()
        {
            using var source = new IntColumn("Src", 3);
            source.Append(10); source.Append(20); source.Append(30);

            var view = new IndirectColumn<int>(source, new[] { 2, 0 }); // [30, 10]

            // Act: Clone subset of the view (take first element -> 30)
            var materialized = view.CloneSubset(new[] { 0 });

            // Assert: Should be a real IntColumn now, not Indirect
            Assert.IsType<IntColumn>(materialized);
            Assert.Equal(1, materialized.Length);
            Assert.Equal(30, ((IntColumn)materialized).Get(0));
        }

        [Fact]
        public void Unsupported_Operations_Throw()
        {
            using var source = new IntColumn("Src", 1);
            var view = new IndirectColumn<int>(source, new[] { 0 });

            Assert.Throws<NotSupportedException>(() => view.Append(1));
            Assert.Throws<NotSupportedException>(() => view.EnsureCapacity(10));
            Assert.Throws<NotSupportedException>(() => _ = view.Values); // No Span support
        }
    }
}