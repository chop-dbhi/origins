package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/view"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/cors"
	"github.com/sirupsen/logrus"
)

const (
	defaultFormat = "json"

	StatusUnprocessableEntity = 422
)

var (
	httpEngine storage.Engine

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

func Serve(engine storage.Engine, host string, port int) {
	// Set reference to shared storage pointer.
	httpEngine = engine

	addr := fmt.Sprintf("%s:%d", host, port)

	// Bind the routes.
	router := httprouter.New()

	router.GET("/", httpRoot)
	router.GET("/domains", httpDomains)

	router.GET("/log/:domain", httpLog)
	router.GET("/log/:domain/entities", httpDomainEntities)
	router.GET("/log/:domain/attributes", httpDomainAttributes)
	router.GET("/log/:domain/values", httpDomainValues)

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

func httpRoot(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	jsonResponse(w, map[string]interface{}{
		"Title":   "Origins HTTP Service",
		"Version": origins.Version,
	})
}

func httpLog(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	domain := p.ByName("domain")

	iter, err := domainIteratorResource(domain, w, r, p)

	if err != nil {
		return
	}

	var fw origins.Writer

	format := detectFormat(w, r)

	switch format {
	case "text", "csv":
		fw = origins.NewCSVWriter(w)

		if _, err := origins.Copy(iter, fw); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprint(err)))
			return
		}

	case "json":
		encoder := json.NewEncoder(w)

		facts, err := origins.ReadAll(iter)

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprint(err)))
			return
		}

		if err = encoder.Encode(facts); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprint(err)))
			return
		}

	default:
		w.WriteHeader(http.StatusNotAcceptable)
		return
	}
}

func httpTimeline(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	domain := p.ByName("domain")

	iter, err := domainIteratorResource(domain, w, r, p)

	if err != nil {
		return
	}

	events, err := view.Timeline(iter, view.Descending)

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

func httpDomains(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	domain := "origins.domains"

	iter, err := domainIteratorResource(domain, w, r, p)

	if err != nil {
		return
	}

	// Extract the domain names.
	idents, err := view.Entities(iter)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprint(err)))
		return
	}

	names := make([]string, len(idents))

	for i, id := range idents {
		names[i] = id.Name
	}

	format := detectFormat(w, r)

	switch format {
	case "json":
		b, err := json.Marshal(names)

		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprint(err)))
			return
		}

		w.Write(b)

	default:
		w.Write([]byte(strings.Join(names, "\n")))
		return
	}
}

func httpDomainEntities(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	domain := p.ByName("domain")

	iter, err := domainIteratorResource(domain, w, r, p)

	if err != nil {
		return
	}

	idents, err := view.Entities(iter)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprint(err)))
		return
	}

	writeIdents(idents, w, r)
}

func httpDomainAttributes(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	domain := p.ByName("domain")

	iter, err := domainIteratorResource(domain, w, r, p)

	if err != nil {
		return
	}

	idents, err := view.Attributes(iter)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprint(err)))
		return
	}

	writeIdents(idents, w, r)
}

func httpDomainValues(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	domain := p.ByName("domain")

	iter, err := domainIteratorResource(domain, w, r, p)

	if err != nil {
		return
	}

	idents, err := view.Values(iter)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprint(err)))
		return
	}

	writeIdents(idents, w, r)
}
