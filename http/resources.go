package http

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/view"
	"github.com/labstack/echo"
)

const StatusUnprocessableEntity = 422

func httpRoot(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]interface{}{
		"Title":   "Origins HTTP Service",
		"Version": origins.Version,
	})
}

func httpDomains(c *echo.Context) error {
	r := c.Request()
	e := c.Get("engine").(storage.Engine)

	domain := "origins.domains"

	iter, code, err := domainIteratorResource(domain, r, e)

	// Special case, just show an empty list.
	if err == view.ErrDoesNotExist {
		return c.JSON(http.StatusOK, []string{})
	}

	if err != nil {
		return c.JSON(code, map[string]interface{}{
			"error": fmt.Sprint(err),
		})
	}

	// Extract the domain names.
	idents, err := origins.Entities(iter)

	if err != nil {
		return c.JSON(code, map[string]interface{}{
			"error": fmt.Sprint(err),
		})
	}

	names := make([]string, len(idents))

	for i, id := range idents {
		names[i] = id.Name
	}

	return c.JSON(http.StatusOK, names)
}

func httpLog(c *echo.Context) error {
	r := c.Request()
	w := c.Response()
	e := c.Get("engine").(storage.Engine)

	domain := c.Param("domain")

	iter, code, err := domainIteratorResource(domain, r, e)

	if err != nil {
		return c.JSON(code, map[string]interface{}{
			"error": fmt.Sprint(err),
		})
	}

	facts, err := origins.ReadAll(iter)

	if err != nil {
		return c.JSON(code, map[string]interface{}{
			"error": fmt.Sprint(err),
		})
	}

	return json.NewEncoder(w).Encode(facts)
}

func httpTimeline(c *echo.Context) error {
	r := c.Request()
	w := c.Response()
	e := c.Get("engine").(storage.Engine)

	domain := c.Param("domain")

	iter, code, err := domainIteratorResource(domain, r, e)

	if err != nil {
		return c.JSON(code, map[string]interface{}{
			"error": fmt.Sprint(err),
		})
	}

	events, err := view.Timeline(iter, view.Descending)

	if err != nil {
		return c.JSON(code, map[string]interface{}{
			"error": fmt.Sprint(err),
		})
	}

	return json.NewEncoder(w).Encode(events)
}

func httpDomainEntities(c *echo.Context) error {
	r := c.Request()
	e := c.Get("engine").(storage.Engine)

	domain := c.Param("domain")

	iter, code, err := domainIteratorResource(domain, r, e)

	if err != nil {
		return c.JSON(code, map[string]interface{}{
			"error": fmt.Sprint(err),
		})
	}

	idents, err := origins.Entities(iter)

	if err != nil {
		return c.JSON(code, map[string]interface{}{
			"error": fmt.Sprint(err),
		})
	}

	return c.JSON(http.StatusOK, idents)
}

func httpDomainAttributes(c *echo.Context) error {
	r := c.Request()
	e := c.Get("engine").(storage.Engine)

	domain := c.Param("domain")

	iter, code, err := domainIteratorResource(domain, r, e)

	if err != nil {
		return c.JSON(code, map[string]interface{}{
			"error": fmt.Sprint(err),
		})
	}

	idents, err := origins.Attributes(iter)

	if err != nil {
		return c.JSON(code, map[string]interface{}{
			"error": fmt.Sprint(err),
		})
	}

	return c.JSON(http.StatusOK, idents)
}

func httpDomainValues(c *echo.Context) error {
	r := c.Request()
	e := c.Get("engine").(storage.Engine)

	domain := c.Param("domain")

	iter, code, err := domainIteratorResource(domain, r, e)

	if err != nil {
		return c.JSON(code, map[string]interface{}{
			"error": fmt.Sprint(err),
		})
	}

	idents, err := origins.Values(iter)

	if err != nil {
		return c.JSON(code, map[string]interface{}{
			"error": fmt.Sprint(err),
		})
	}

	return c.JSON(http.StatusOK, idents)
}
