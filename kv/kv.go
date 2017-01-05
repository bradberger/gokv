package kv

import (
	"errors"
	"fmt"
)

// Errors
var (
	ErrNotFound  = errors.New("not found")
	ErrCacheMiss = errors.New("cache miss")
	// ErrInvalidDstVal is returned with the dstVal of the Get() func cannot be set. It's probably because the
	// dstVal is not a pointer.
	ErrInvalidDstVal = errors.New("cannot set dst value")
	// ErrInvalidDataFormat is returned when the data retrieved from a storage engine is not in the expected format
	ErrInvalidDataFormat = errors.New("Invalid data format")
)

// Key turns Stringer funcs, byte slices, pointers to strings, etc., into string keys
func Key(key interface{}) string {
	if kp, ok := key.(KeyProvider); ok {
		return kp.Key()
	}
	switch key.(type) {
	case string:
		return key.(string)
	case *string:
		return *key.(*string)
	case []byte:
		return string(key.([]byte))
	case *[]byte:
		return string(*key.(*[]byte))
	default:
		return fmt.Sprintf("%#v", key)
	}
}

// Store defines a permanent key/value store
type Store interface {
	Set(key string, value interface{}) error
	Get(key string, dstVal interface{}) error
	Del(key string) error
}

// Clearer defines an interface which store can clear all it's key/value pairs at once.
type Clearer interface {
	Clear() error
}

// KeyList defines an interface for announcing all keys currently set
type KeyList interface {
	Keys() []string
}

// Datastore defines an key/value interface which supports exporting all it's keys and also
// transferring all it's data to another KeyStore.
type Datastore interface {
	Store
	KeyList
	Transfer(Store) error
}

// KeyProvider is an interface which can describe it's own key. It's used for getting/setting
// key/value pairs without a directly supplied key string. Instead, the supplied interface
// can announce it's own key, and that's used in getting/setting.
type KeyProvider interface {
	Key() string
}
