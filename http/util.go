package http

import (
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/chrono"
	"github.com/chop-dbhi/origins/view"
	"github.com/julienschmidt/httprouter"
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
func domainIteratorResource(domain string, w http.ResponseWriter, r *http.Request, p httprouter.Params) (origins.Iterator, error) {
	var (
		err           error
		since, asof   time.Time
		offset, limit int
	)

	if since, asof, err = parseTimeParams(r); err != nil {
		w.WriteHeader(StatusUnprocessableEntity)
		w.Write([]byte(fmt.Sprint(err)))
		return nil, err
	}

	if offset, limit, err = parseSliceParams(r); err != nil {
		w.WriteHeader(StatusUnprocessableEntity)
		w.Write([]byte(fmt.Sprint(err)))
		return nil, err
	}

	log, err := view.OpenLog(httpEngine, domain, "commit")

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprint(err)))
		return nil, err
	}

	iter := log.View(since, asof)

	if offset > 0 || limit > 0 {
		iter = origins.Slice(iter, offset, limit)
	}

	return iter, nil
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

// jsonResponse attempts to encode the passed value as JSON.
func jsonResponse(w http.ResponseWriter, v interface{}) {
	e := json.NewEncoder(w)

	if err := e.Encode(v); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprint(err)))
	}
}

func writeIdents(idents origins.Idents, w http.ResponseWriter, r *http.Request) {
	format := detectFormat(w, r)

	switch format {
	case "json":
		encoder := json.NewEncoder(w)

		if err := encoder.Encode(idents); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprint(err)))
			return
		}

	default:
		w.WriteHeader(http.StatusNotAcceptable)
		return
	}
}
