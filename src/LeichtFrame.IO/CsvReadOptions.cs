using System.Globalization;

namespace LeichtFrame.IO
{
    public class CsvReadOptions
    {
        public string Separator { get; set; } = ",";
        public bool HasHeader { get; set; } = true;
        public CultureInfo Culture { get; set; } = CultureInfo.InvariantCulture;
        public string? DateFormat { get; set; } = null; // null = Auto/Default
    }
}