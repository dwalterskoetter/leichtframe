using System.Globalization;

namespace LeichtFrame.IO
{
    public class CsvWriteOptions
    {
        public string Separator { get; set; } = ",";
        public bool WriteHeader { get; set; } = true;

        // Standard: Always Invariant (dot instead of comma) to ensure compatibility
        public CultureInfo Culture { get; set; } = CultureInfo.InvariantCulture;

        // ISO 8601 "o" is the safest standard for date/time (roundtrip-capable)
        public string DateFormat { get; set; } = "o";

        public string NullValue { get; set; } = "";
    }
}