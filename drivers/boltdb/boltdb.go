package boltdb

import (
	"os"

	"github.com/boltdb/bolt"
	"github.com/bradberger/gokv/codec"
	"github.com/bradberger/gokv/kv"
)

var (
	// Codec is the codec used to marshal/unmarshal interfaces into the byte slices required by the BoltDB client.
	// The default codec is Gob
	Codec codec.Codec

	// ensure struct implements the kv.Store interface
	_ kv.Store = (*DB)(nil)
)

func init() {
	Codec = codec.Gob
}

// DB is a struct which implements the "kv.Store" interface powered by BoltDB under the hood
type DB struct {
	db     *bolt.DB
	bucket string
}

// New creates a new DB struct to interace with the underlying BoltDB database. Be sure to close it when you're done or it could hang
// If the bucket does not exist, it will be created.
func New(path string, bucket string, mode os.FileMode, options *bolt.Options) (*DB, error) {
	var err error
	db, err := bolt.Open(path, mode, options)
	if err != nil {
		return nil, err
	}
	return &DB{db: db, bucket: bucket}, db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucket))
		return err
	})
}

// Set implements the "kv.Store".Set() interface
func (d *DB) Set(key string, value interface{}) error {
	b, err := Codec.Marshal(value)
	if err != nil {
		return err
	}
	return d.DB().Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(d.bucket)).Put([]byte(key), b)
	})
}

// Get implements the "kv.Store".Get() interface
func (d *DB) Get(key string, dstVal interface{}) error {
	return d.DB().View(func(tx *bolt.Tx) error {
		val := tx.Bucket([]byte(d.bucket)).Get([]byte(key))
		if val == nil {
			return kv.ErrNotFound
		}
		return Codec.Unmarshal(val, dstVal)
	})
}

// Del implements the "kv.Store".Del() interface
func (d *DB) Del(key string) error {
	return d.DB().Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(d.bucket)).Delete([]byte(key))
	})
}

// DB returns the underling BoltDB struct
func (d *DB) DB() *bolt.DB {
	return d.db
}

// Close closes the underlying BoltDB database file
func (d *DB) Close() error {
	return d.DB().Close()
}

// Bucket returns the name of the BoltDB bucket in use
func (d *DB) Bucket() string {
	return d.bucket
}
