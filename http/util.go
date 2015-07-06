package http

import (
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/chrono"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/view"
)

const defaultFormat = "json"

var (
	mimetypes = map[string]string{
		"application/json": "json",
		"text/csv":         "csv",
		"text/plain":       "text",
	}

	formatMimetypes = map[string]string{
		"csv":  "text/csv",
		"json": "application/json",
		"text": "text/plain",
	}

	queryFormats = map[string]string{
		"json": "json",
		"csv":  "csv",
		"text": "text",
	}
)

// detectFormat applies content negotiation logic to determine the
// appropriate response representation.
func detectFormat(w http.ResponseWriter, r *http.Request) string {
	var (
		ok     bool
		format string
	)

	format = queryFormats[strings.ToLower(r.URL.Query().Get("format"))]

	// Query parameter
	if format == "" {
		// Accept header
		acceptType := r.Header.Get("Accept")
		acceptType, _, _ = mime.ParseMediaType(acceptType)

		// Fallback to default
		if format, ok = mimetypes[acceptType]; !ok {
			format = defaultFormat
		}
	}

	w.Header().Set("content-type", formatMimetypes[format])

	return format
}

// Encapsulates the logic for building a domain-based iterator.
func domainIteratorResource(domain string, r *http.Request, e storage.Engine) (origins.Iterator, int, error) {
	var (
		err           error
		since, asof   time.Time
		offset, limit int
	)

	if since, asof, err = parseTimeParams(r); err != nil {
		return nil, StatusUnprocessableEntity, err
	}

	if offset, limit, err = parseSliceParams(r); err != nil {
		return nil, StatusUnprocessableEntity, err
	}

	log, err := view.OpenLog(e, domain, "commit")

	if err == view.ErrDoesNotExist {
		return nil, http.StatusNotFound, err
	}

	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	iter := log.View(since, asof)

	if offset > 0 || limit > 0 {
		iter = origins.Slice(iter, offset, limit)
	}

	return iter, http.StatusOK, nil
}

// Parses the since and asof time values from the request.
func parseTimeParams(r *http.Request) (time.Time, time.Time, error) {
	var (
		err         error
		since, asof time.Time
	)

	q := r.URL.Query()

	// Parse query parameters
	if q.Get("since") != "" {
		if since, err = chrono.Parse(q.Get("since")); err != nil {
			return since, asof, err
		}
	}

	if q.Get("asof") != "" {
		if asof, err = chrono.Parse(q.Get("asof")); err != nil {
			return since, asof, err
		}
	}

	return since, asof, nil
}

// Parses the offset and limit from the request.
func parseSliceParams(r *http.Request) (int, int, error) {
	var (
		err           error
		offset, limit int
	)

	q := r.URL.Query()

	if q.Get("offset") != "" {
		if offset, err = strconv.Atoi(q.Get("offset")); err != nil {
			return limit, offset, err
		}
	}

	if q.Get("limit") != "" {
		if limit, err = strconv.Atoi(q.Get("limit")); err != nil {
			return limit, offset, err
		}
	}

	return offset, limit, nil
}
