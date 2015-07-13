package http

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/chop-dbhi/origins/storage"
	"github.com/labstack/echo"
	mw "github.com/labstack/echo/middleware"
	"github.com/rs/cors"
)

type Server struct {
	Host         string
	Port         int
	Debug        bool
	Engine       storage.Engine
	AllowedHosts []string

	core *echo.Echo
}

// setup prepares the internal HTTP handle, middleware, and resources.
func (s *Server) setup() {
	e := echo.New()
	s.core = e

	// Enable HTTP 2
	e.HTTP2(true)

	// Toggle debug
	e.SetDebug(s.Debug)

	// Setup middleware.
	e.Use(mw.Logger())
	e.Use(mw.Recover())
	e.Use(mw.Gzip())

	// Setup CORS.
	e.Use(cors.New(cors.Options{
		AllowedOrigins: s.AllowedHosts,
	}).Handler)

	// Add middleware for setting the server context.
	e.Use(s.serverContext)

	e.Get("/", httpRoot)
	e.Get("/domains", httpDomains)

	e.Get("/log/:domain", httpLog)
	e.Get("/log/:domain/entities", httpDomainEntities)
	e.Get("/log/:domain/attributes", httpDomainAttributes)
	e.Get("/log/:domain/values", httpDomainValues)

	e.Get("/timeline/:domain", httpTimeline)
}

func (s *Server) serverContext(c *echo.Context) error {
	c.Set("engine", s.Engine)
	return nil
}

func (s *Server) Serve() {
	s.setup()

	// Serve it up.
	addr := fmt.Sprintf("%s:%d", s.Host, s.Port)
	logrus.Infof("* Listening on %s...", addr)

	s.core.Run(addr)
}
