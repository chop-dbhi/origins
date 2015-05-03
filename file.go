package origins

import "regexp"

// DetectFormat detects the file format based on the filename.
func DetectFormat(n string) string {
	var ok bool

	if ok, _ = regexp.MatchString(`(?i)\.csv\b`, n); ok {
		return "csv"
	} else if ok, _ = regexp.MatchString(`(?i)\.jsonstream\b`, n); ok {
		return "jsonstream"
	}

	return ""
}

// DetectCompression detects the file compression type based on the filename.
func DetectCompression(n string) string {
	var ok bool

	if ok, _ = regexp.MatchString(`(?i)\.gz\b`, n); ok {
		return "gzip"
	} else if ok, _ = regexp.MatchString(`(?i)\.(bzip2|bz2)\b`, n); ok {
		return "bzip2"
	}

	return ""
}
