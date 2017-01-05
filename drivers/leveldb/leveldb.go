package leveldb

import (
	"github.com/bradberger/gokv/codec"
	"github.com/bradberger/gokv/kv"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	// Codec is the codec used to marshal/unmarshal interfaces into the byte slices required by the Diskv client
	Codec codec.Codec

	// ensure struct implements the kv.Store interface
	_ kv.Store = (*DB)(nil)
)

func init() {
	Codec = codec.Gob
}

// DB is a light wrapper around the leveldb struct
type DB struct {
	db *leveldb.DB
}

// New returns a new key/value store powered by leveldb
func New(file string, opts *opt.Options) (*DB, error) {
	db, err := leveldb.OpenFile(file, opts)
	if err != nil {
		return nil, err
	}
	return &DB{db: db}, nil
}

// Get implements the "kv.Store".Get interface
func (db *DB) Get(key string, dstVal interface{}) error {
	b, err := db.DB().Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			err = kv.ErrNotFound
		}
		return err
	}
	return Codec.Unmarshal(b, dstVal)
}

// Set implements the "kv.Store".Set interface
func (db *DB) Set(key string, val interface{}) error {
	b, err := Codec.Marshal(val)
	if err != nil {
		return err
	}
	return db.DB().Put([]byte(key), b, nil)
}

// Del implements the "kv.Store".Del interface
func (db *DB) Del(key string) error {
	return db.DB().Delete([]byte(key), nil)
}

// DB returns the underlying LevelDB database
func (db *DB) DB() *leveldb.DB {
	return db.db
}

// Close is used to close the database.
func (db *DB) Close() error {
	return db.DB().Close()
}
