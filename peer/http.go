package peer

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/storage/boltdb"
	"github.com/chop-dbhi/origins/storage/disk"
	"github.com/chop-dbhi/origins/storage/memory"
	"github.com/chop-dbhi/origins/storage/sqlite"
	"github.com/chop-dbhi/origins/view"
	"github.com/julienschmidt/httprouter"
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
	case "diskv":
		engine, err = disk.Open(&opts)
	case "boltdb":
		engine, err = boltdb.Open(&opts)
	case "sqlite":
		engine, err = sqlite.Open(&opts)
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
	host := viper.GetString("http.host")
	port := viper.GetInt("http.port")

	addr := fmt.Sprintf("%s:%d", host, port)

	// Bind the routes.
	router := httprouter.New()

	router.GET("/", httpRoot)
	router.GET("/stats", httpStats)
	router.GET("/domains", httpDomains)
	router.GET("/domains/:ident", httpDomain)
	router.GET("/domains/:ident/stats", httpDomainStats)
	router.GET("/domains/:ident/entities", httpDomainEntities)
	router.GET("/domains/:ident/attributes", httpDomainAttributes)
	router.GET("/domains/:ident/values", httpDomainValues)
	router.GET("/domains/:ident/facts", httpDomainFacts)

	// Serve it up.
	logrus.Infof("* Listening on %s...", addr)

	logrus.Fatal(http.ListenAndServe(addr, router))
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
	//d := p.ByName("ident")

	v := view.Now(store).Domain("origins.domains")

	jsonResponse(w, v.Stats())
}

func httpDomainStats(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	d := p.ByName("ident")

	v := view.Now(store).Domain(d)

	jsonResponse(w, v.Stats())
}

func httpDomainEntities(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	d := p.ByName("ident")

	v := view.Now(store).Domain(d)

	jsonResponse(w, v.Entities())
}

func httpDomainAttributes(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	d := p.ByName("ident")

	v := view.Now(store).Domain(d)

	jsonResponse(w, v.Attributes())
}

func httpDomainValues(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	d := p.ByName("ident")

	v := view.Now(store).Domain(d)

	jsonResponse(w, v.Values())
}

func httpDomainFacts(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	d := p.ByName("ident")

	v := view.Now(store).Domain(d)

	jsonResponse(w, v.Facts())
}
