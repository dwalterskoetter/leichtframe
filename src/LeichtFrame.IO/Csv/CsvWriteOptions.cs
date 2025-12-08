using System.Globalization;

namespace LeichtFrame.IO
{
    /// <summary>
    /// Configuration options for writing CSV files.
    /// </summary>
    public class CsvWriteOptions
    {
        /// <summary>
        /// Gets or sets the delimiter used to separate fields. Default is ",".
        /// </summary>
        public string Separator { get; set; } = ",";

        /// <summary>
        /// Gets or sets a value indicating whether to write column names as the first row. 
        /// Default is <c>true</c>.
        /// </summary>
        public bool WriteHeader { get; set; } = true;

        /// <summary>
        /// Gets or sets the culture information used to format numbers and dates.
        /// Default is <see cref="CultureInfo.InvariantCulture"/> (dot decimal separator) to ensure compatibility.
        /// </summary>
        public CultureInfo Culture { get; set; } = CultureInfo.InvariantCulture;

        /// <summary>
        /// Gets or sets the format string for <see cref="DateTime"/> values.
        /// Default is "o" (ISO 8601 round-trip pattern), which is the safest standard for machine processing.
        /// </summary>
        public string DateFormat { get; set; } = "o";

        /// <summary>
        /// Gets or sets the string representation for null values. 
        /// Default is an empty string.
        /// </summary>
        public string NullValue { get; set; } = "";
    }
}