package main

import (
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/chrono"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/view"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/cors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var httpCmd = &cobra.Command{
	Use: "http",

	Short: "Starts an HTTP peer.",

	Long: "Runs a process exposing an HTTP interface.",

	Run: func(cmd *cobra.Command, args []string) {
		var (
			host = viper.GetString("http_host")
			port = viper.GetInt("http_port")
		)

		engine := initStorage()

		serveHTTP(engine, host, port)
	},
}

func init() {
	flags := httpCmd.Flags()

	flags.String("host", "", "The host the HTTP service will listen on.")
	flags.Int("port", 49110, "The port the HTTP will bind to.")

	viper.BindPFlag("http_host", flags.Lookup("host"))
	viper.BindPFlag("http_port", flags.Lookup("port"))
}

const (
	defaultFormat = "json"

	StatusUnprocessableEntity = 422
)

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

func serveHTTP(engine storage.Engine, host string, port int) {
	addr := fmt.Sprintf("%s:%d", host, port)

	// Bind the routes.
	router := httprouter.New()

	router.GET("/", httpRoot)
	router.GET("/log/:domain", httpLogView)
	router.GET("/timeline/:domain", httpTimeline)

	// Add CORS middleware
	c := cors.New(cors.Options{
		ExposedHeaders: []string{
			"Link",
			"Link-Template",
		},
	})

	handler := c.Handler(router)

	// Serve it up.
	logrus.Infof("* Listening on %s...", addr)

	logrus.Fatal(http.ListenAndServe(addr, handler))
}

// jsonResponse attempts to encode the passed value as JSON.
func jsonResponse(w http.ResponseWriter, v interface{}) {
	e := json.NewEncoder(w)

	if err := e.Encode(v); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprint(err)))
	}
}

func httpRoot(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	jsonResponse(w, map[string]interface{}{
		"Title":   "Origins HTTP Service",
		"Version": origins.Version,
	})
}

func httpLogView(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	domain := p.ByName("domain")

	var (
		err         error
		since, asof time.Time
	)

	q := r.URL.Query()

	// Parse query parameters
	if q.Get("since") != "" {
		since, err = chrono.Parse(q.Get("since"))

		if err != nil {
			w.WriteHeader(StatusUnprocessableEntity)
			w.Write([]byte(fmt.Sprint(err)))
			return
		}
	}

	if q.Get("asof") != "" {
		asof, err = chrono.Parse(q.Get("asof"))

		if err != nil {
			w.WriteHeader(StatusUnprocessableEntity)
			w.Write([]byte(fmt.Sprint(err)))
			return
		}
	}

	// Open the log.
	engine := initStorage()

	log, err := view.OpenLog(engine, domain, "commit")

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprint(err)))
		return
	}

	// Construct a view of the log for the specified window of time.
	v := log.View(since, asof)

	var fw origins.Writer

	format := detectFormat(w, r)

	switch format {
	case "text", "csv":
		fw = origins.NewCSVWriter(w)

	case "json":
		encoder := json.NewEncoder(w)

		facts, err := origins.ReadAll(v)

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprint(err)))
			return
		}

		if err = encoder.Encode(facts); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprint(err)))
		}

		return

	default:
		w.WriteHeader(http.StatusNotAcceptable)
		return
	}

	if _, err := origins.Copy(v, fw); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprint(err)))
		return
	}
}

func httpTimeline(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	domain := p.ByName("domain")

	var (
		err         error
		since, asof time.Time
	)

	q := r.URL.Query()

	// Parse query parameters
	if q.Get("since") != "" {
		since, err = chrono.Parse(q.Get("since"))

		if err != nil {
			w.WriteHeader(StatusUnprocessableEntity)
			w.Write([]byte(fmt.Sprint(err)))
			return
		}
	}

	if q.Get("asof") != "" {
		asof, err = chrono.Parse(q.Get("asof"))

		if err != nil {
			w.WriteHeader(StatusUnprocessableEntity)
			w.Write([]byte(fmt.Sprint(err)))
			return
		}
	}

	// Open the log.
	engine := initStorage()

	log, err := view.OpenLog(engine, domain, "commit")

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprint(err)))
		return
	}

	// Construct a view of the log for the specified window of time.
	v := log.View(since, asof)

	events, err := view.Timeline(v, view.Descending)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprint(err)))
		return
	}

	encoder := json.NewEncoder(w)

	if err = encoder.Encode(events); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprint(err)))
		return
	}
}
