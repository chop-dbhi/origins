package transactor

import "github.com/chop-dbhi/origins"

// Router is an interface for routing facts to pipelines.
type Router interface {
	// Route takes a fact and returns the pipeline to which should handle
	// the fact. An error is also included in case
	Route(fact *origins.Fact) (Pipeline, error)
}

// DomainRouter routes facts by their domain.
type DomainRouter struct {
	// Keep track of initialized pipelines by domain.
	pipes map[string]*DomainPipeline
}

// Route initializes a DomainPipeline for the fact's domain.
func (r *DomainRouter) Route(fact *origins.Fact) (Pipeline, error) {
	if pipe, ok := r.pipes[fact.Domain]; ok {
		return pipe, nil
	}

	pipe := DomainPipeline{
		Domain: fact.Domain,
	}

	r.pipes[fact.Domain] = &pipe

	return &pipe, nil
}

// NewDomainRouter returns a domain router.
func NewDomainRouter() *DomainRouter {
	return &DomainRouter{
		pipes: make(map[string]*DomainPipeline),
	}
}
