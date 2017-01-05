package datastore

import (
	"github.com/bradberger/gokv/kv"
	"golang.org/x/net/context"
	ae "google.golang.org/appengine/datastore"
)

var (
	// ensure Entity struct implements the kv.Store interface
	_ kv.Store = (*Entity)(nil)
)

// Entity represents the entity being stored.
type Entity struct {
	Context context.Context
	Entity  string
}

// New returns a new entity with the given context and entityName
func New(ctx context.Context, entityName string) *Entity {
	return &Entity{ctx, entityName}
}

// WithContext sets the internal context
func (e *Entity) WithContext(ctx context.Context) *Entity {
	e.Context = ctx
	return e
}

// Set implements the "kv.Cache".Set() interface
func (e *Entity) Set(key string, value interface{}) (err error) {
	_, err = ae.Put(e.Context, e.Key(key), value)
	return
}

// Get implements the "kv.Cache".Get() interface
func (e *Entity) Get(key string, dstVal interface{}) error {
	return ae.Get(e.Context, e.Key(key), dstVal)
}

// Del implements the "kv.Cache".Del() interface
func (e *Entity) Del(key string) error {
	return ae.Delete(e.Context, e.Key(key))
}

// Key returns the datastore Key string associated with the entity
func (e *Entity) Key(key string) *ae.Key {
	return ae.NewKey(e.Context, e.Entity, key, 0, nil)
}
