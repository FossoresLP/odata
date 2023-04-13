package odata

import (
	"fmt"
	"strings"

	"github.com/go-resty/resty/v2"
)

type RequestError struct {
	Status     int
	StatusText string
	Body       string
}

func (r *RequestError) Error() string {
	return fmt.Sprintf("request failed with status %d - %s: %q", r.Status, r.StatusText, r.Body)
}

// RequestProviders generate Resty requests that already contain a base URL and the necessary authentication information for the OData API
type RequestProvider interface {
	NewRequest() (*resty.Request, error)
}

type Respose[V any] struct {
	Context string `json:"@odata.context"`
	Count   uint64 `json:"@odata.count"`
	Next    string `json:"@odata.nextLink"`
	Value   []V    `json:"value"`
}

// Result returns the data contained in the OData response
func (o *Respose[V]) Result() []V {
	return o.Value
}

// Collect iterates through pages of OData results and collects them into the original result
func (o *Respose[V]) Collect(c RequestProvider) error {
	for last := o; len(last.Next) > 0; {
		req, err := c.NewRequest()
		if err != nil {
			return err
		}
		result := Respose[V]{}
		res, err := req.SetResult(&result).Get(last.Next)
		if err != nil {
			return err
		}
		if res.IsError() {
			return &RequestError{res.StatusCode(), res.Status(), res.String()}
		}
		o.Value = append(o.Value, result.Result()...)
		last = &result
	}
	return nil
}

type Direction uint8

const (
	Ascending Direction = iota
	Descending
)

type Order map[string]Direction

func (o *Order) String() string {
	sb := strings.Builder{}
	for k, v := range *o {
		if sb.Len() > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(k)
		switch v {
		case Ascending:
			sb.WriteString(" asc")
		case Descending:
			sb.WriteString(" desc")
		}
	}
	return sb.String()
}

// Query is a builder type for an OData query
type Query[V any] struct {
	client     RequestProvider
	url        string
	count      bool
	expand     []string
	filter     string
	orderBy    Order
	search     string
	selectKeys []string
	skip       uint64
	top        uint64
	pathParams map[string]string
}

// NewQuery creates a new OData query for a specific URL that will be resolved with the provided RequestProvider
func NewQuery[V any](client RequestProvider, url string) *Query[V] {
	return &Query[V]{
		client:     client,
		url:        url,
		pathParams: make(map[string]string),
		orderBy:    make(Order),
	}
}

// Count requests a count to be added to the OData response
func (o *Query[V]) Count() *Query[V] {
	o.count = true
	return o
}

// Expand selects which fields should be expanded
func (o *Query[V]) Expand(keys ...string) *Query[V] {
	o.expand = keys
	return o
}

// Filter sets a filter expression used to filter results
func (o *Query[V]) Filter(filter string) *Query[V] {
	o.filter = filter
	return o
}

// OrderBy defines the key and direction to order the results by
func (o *Query[V]) OrderBy(key string, direction ...Direction) *Query[V] {
	if len(direction) > 0 {
		o.orderBy[key] = direction[0]
	} else {
		o.orderBy[key] = 255
	}
	return o
}

// Search performs a search
func (o *Query[V]) Search(query string) *Query[V] {
	o.search = query
	return o
}

// Select defines the keys to request
func (o *Query[V]) Select(keys ...string) *Query[V] {
	o.selectKeys = keys
	return o
}

// Skip sets how many results should be skipped
func (o *Query[V]) Skip(num uint64) *Query[V] {
	o.skip = num
	return o
}

// Top limits the number of results
func (o *Query[V]) Top(num uint64) *Query[V] {
	o.top = num
	return o
}

// PathParam sets a path parameter for the OData query
func (o *Query[V]) PathParam(key, value string) *Query[V] {
	o.pathParams[key] = value
	return o
}

// Prepare creates a new OData request using the RequestProvider and sets the queries according to the builder functions
func (o *Query[V]) prepare() (*resty.Request, error) {
	r, err := o.client.NewRequest()
	if err != nil {
		return nil, err
	}
	if o.count {
		r.SetQueryParam("$count", "true")
	}
	if len(o.expand) > 0 {
		r.SetQueryParam("$expand", strings.Join(o.expand, ","))
	}
	if len(o.filter) > 0 {
		r.SetQueryParam("$filter", o.filter)
	}
	if len(o.orderBy) > 0 {
		r.SetQueryParam("$orderby", o.orderBy.String())
	}
	if len(o.search) > 0 {
		r.SetQueryParam("$search", o.search)
	}
	if len(o.selectKeys) > 0 {
		r.SetQueryParam("$select", strings.Join(o.selectKeys, ","))
	}
	return r.SetPathParams(o.pathParams), nil
}

// Get performs a simple get on an OData API returning a single item
func (o *Query[V]) Get() (*V, error) {
	r, err := o.prepare()
	if err != nil {
		return nil, err
	}
	result := new(V)
	res, err := r.SetResult(result).Get(o.url)
	if err != nil {
		return nil, err
	}
	if res.IsError() {
		return nil, &RequestError{res.StatusCode(), res.Status(), res.String()}
	}
	return result, nil
}

// GetAll performs an OData request for a set of items, iterating through all pages and collecting the results
func (o *Query[V]) GetAll() ([]V, error) {
	r, err := o.prepare()
	if err != nil {
		return nil, err
	}
	result := Respose[V]{}
	res, err := r.SetResult(&result).Get(o.url)
	if err != nil {
		return nil, err
	}
	if res.IsError() {
		return nil, &RequestError{res.StatusCode(), res.Status(), res.String()}
	}
	err = result.Collect(o.client)
	if err != nil {
		return nil, err
	}
	return result.Value, nil
}
