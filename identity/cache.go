package identity

// Cache is a map of domain -> local -> *Ident. This is useful
// during parsing to prevent redundant identifiers being initialized.
type Cache map[string]map[string]*Ident

func (c *Cache) Add(domain, local string) *Ident {
	m := *c

	if _, ok := m[domain]; !ok {
		m[domain] = make(map[string]*Ident)
	}

	d := m[domain]

	if _, ok := d[local]; !ok {
		d[local] = &Ident{
			Domain: domain,
			Local:  local,
		}
	}

	return d[local]
}

func (c *Cache) AddIdent(i *Ident) *Ident {
	m := *c

	if _, ok := m[i.Domain]; !ok {
		m[i.Domain] = make(map[string]*Ident)
	}

	d := m[i.Domain]

	// Only add if this is new so we don't get duplicate values.
	if _, ok := d[i.Local]; !ok {
		d[i.Local] = i
	}

	return d[i.Local]
}

func (c *Cache) Has(domain, local string) bool {
	m := *c

	if _, ok := m[domain]; !ok {
		return false
	}

	if _, ok := m[domain][local]; !ok {
		return false
	}

	return true
}

func (c *Cache) HasIdent(i *Ident) bool {
	m := *c

	if _, ok := m[i.Domain]; !ok {
		return false
	}

	if _, ok := m[i.Domain][i.Local]; !ok {
		return false
	}

	return true
}

func (c *Cache) Slice() []*Ident {
	m := *c

	// Initialize empty slice
	var tmp []*Ident
	data := make([]*Ident, 0)
	var i int

	for _, locals := range m {
		// Extend the size of the slice
		tmp = make([]*Ident, i+len(locals))
		copy(tmp, data)
		data = tmp

		for _, id := range locals {
			tmp[i] = id
			i += 1
		}
	}

	return data
}
