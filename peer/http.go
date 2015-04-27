package peer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/identity"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/storage/boltdb"
	"github.com/chop-dbhi/origins/storage/memory"
	"github.com/chop-dbhi/origins/view"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/cors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var store *storage.Store

func ServeHTTP() {
	var (
		err    error
		engine storage.Engine
	)

	opts := storage.Options{
		Path: viper.GetString("path"),
	}

	switch viper.GetString("storage") {
	case "boltdb":
		engine, err = boltdb.Open(&opts)
	case "memory":
		engine, err = memory.Open(&opts)
	default:
		logrus.Fatal("no storage selected")
	}

	if err != nil {
		logrus.Fatal(err)
	}

	// Initialize a store.
	store, err = storage.Init(&storage.Config{
		Engine: engine,
	})

	if err != nil {
		logrus.Fatal(err)
	}

	// Construct the address to bind to.
	host := viper.GetString("http_host")
	port := viper.GetInt("http_port")

	addr := fmt.Sprintf("%s:%d", host, port)

	// Bind the routes.
	router := httprouter.New()

	router.GET("/", httpRoot)
	router.GET("/stats", httpStats)
	router.GET("/domains", httpDomains)
	router.GET("/domains/:domain", httpDomain)
	router.GET("/domains/:domain/stats", httpDomainStats)
	router.GET("/domains/:domain/entities", httpDomainEntities)
	router.GET("/domains/:domain/attributes", httpDomainAttributes)
	router.GET("/domains/:domain/values", httpDomainValues)
	router.GET("/domains/:domain/facts", httpDomainFacts)
	router.GET("/domains/:domain/aggregate/*ident", httpAggregateEntity)

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

func jsonResponse(w http.ResponseWriter, v interface{}) {
	b, err := json.MarshalIndent(v, "", "\t")

	if err != nil {
		panic(err)
	}

	h := w.Header()
	h.Add("Content-Type", "application/json")

	w.Write(b)
}

func httpRoot(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	jsonResponse(w, map[string]interface{}{
		"Title":   "Origins HTTP Service",
		"Version": origins.Version,
	})
}

func httpStats(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	v := view.Now(store).Domain("origins.domains")

	jsonResponse(w, v.Stats())
}

func httpDomains(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	v := view.Now(store).Domain("origins.domains")

	jsonResponse(w, v.Entities())
}

func httpDomain(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	//d := p.ByName("domain")

	v := view.Now(store).Domain("origins.domains")

	jsonResponse(w, v.Stats())
}

func httpDomainStats(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	d := p.ByName("domain")

	v := view.Now(store).Domain(d)

	jsonResponse(w, v.Stats())
}

func httpDomainEntities(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	d := p.ByName("domain")

	v := view.Now(store).Domain(d)

	jsonResponse(w, v.Entities())
}

func httpDomainAttributes(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	d := p.ByName("domain")

	v := view.Now(store).Domain(d)

	jsonResponse(w, v.Attributes())
}

func httpDomainValues(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	d := p.ByName("domain")

	v := view.Now(store).Domain(d)

	jsonResponse(w, v.Values())
}

func httpDomainFacts(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	d := p.ByName("domain")

	v := view.Now(store).Domain(d)

	jsonResponse(w, v.Facts())
}

func httpAggregateEntity(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	d := p.ByName("domain")
	e := p.ByName("ident")

	// Wildcard path matches come with a leading slash.
	e = strings.TrimPrefix(e, "/")

	// Unescape segments.
	e, err := url.QueryUnescape(e)

	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	ident, err := identity.Parse(e)

	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	v := view.Now(store).Domain(d)
	a := v.Aggregate(ident)

	jsonResponse(w, a.Map())
}
