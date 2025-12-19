using System.Globalization;

namespace LeichtFrame.IO
{
    /// <summary>
    /// Configuration options for reading CSV files.
    /// </summary>
    public class CsvReadOptions
    {
        /// <summary>
        /// Gets or sets the delimiter used to separate fields. Default is ",".
        /// </summary>
        public string Separator { get; set; } = ",";

        /// <summary>
        /// Gets or sets a value indicating whether the first row of the CSV contains column headers. 
        /// Default is <c>true</c>.
        /// </summary>
        public bool HasHeader { get; set; } = true;

        /// <summary>
        /// Gets or sets the culture information used to parse numbers and dates. 
        /// Default is <see cref="CultureInfo.InvariantCulture"/> (dot decimal separator).
        /// </summary>
        public CultureInfo Culture { get; set; } = CultureInfo.InvariantCulture;

        /// <summary>
        /// Gets or sets a specific date format string (e.g. "yyyy-MM-dd").
        /// If null (default), the parser attempts to detect the format automatically based on the Culture.
        /// </summary>
        public string? DateFormat { get; set; } = null;
        /// <summary>
        /// Gets or sets a value indicating whether lines have a trailing delimiter. 
        /// Default is <c>false</c>.
        /// </summary>
        public bool HasTrailingDelimiter { get; set; } = false;
    }
}