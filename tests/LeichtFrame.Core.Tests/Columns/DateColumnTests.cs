using System;
using Xunit;
using LeichtFrame.Core;

namespace LeichtFrame.Core.Tests.Columns
{
    public class DateTimeColumnTests
    {
        [Fact]
        public void Basic_Roundtrip_Works()
        {
            using var col = new DateTimeColumn("Dates", 10);
            var now = DateTime.Now;
            var tomorrow = now.AddDays(1);

            col.Append(now);
            col.Append(tomorrow);

            Assert.Equal(2, col.Length);
            Assert.Equal(now, col.GetValue(0));
            Assert.Equal(tomorrow, col.GetValue(1));
        }

        [Fact]
        public void Nullable_Support_Works()
        {
            using var col = new DateTimeColumn("NullableDates", 10, isNullable: true);
            var now = DateTime.UtcNow;

            col.Append(now);
            col.Append((DateTime?)null);

            Assert.False(col.IsNull(0));
            Assert.True(col.IsNull(1));

            // Value at null index should be default(DateTime)
            Assert.Equal(default(DateTime), col.GetValue(1));
        }

        [Fact]
        public void Resizing_Preserves_Data()
        {
            using var col = new DateTimeColumn("Resize", 2);
            col.Append(new DateTime(2023, 1, 1));
            col.Append(new DateTime(2023, 1, 2));
            col.Append(new DateTime(2023, 1, 3)); // Triggers resize

            Assert.Equal(3, col.Length);
            Assert.Equal(new DateTime(2023, 1, 3), col.GetValue(2));
        }

        [Fact]
        public void NonNullable_Throws_On_Null()
        {
            using var col = new DateTimeColumn("Strict", 5, isNullable: false);

            Assert.Throws<InvalidOperationException>(() => col.Append((DateTime?)null));

            col.Append(DateTime.Now);
            Assert.Throws<InvalidOperationException>(() => col.SetNull(0));
        }
    }
}