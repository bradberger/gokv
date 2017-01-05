package diskv

import (
	"strings"

	"github.com/bradberger/gokv/codec"
	"github.com/bradberger/gokv/kv"
	"github.com/peterbourgon/diskv"
)

var (
	// Codec is the codec used to marshal/unmarshal interfaces into the byte slices required by the Diskv client
	Codec codec.Codec

	// ensure struct implements the kv.Store interface
	_ kv.Store = (*Diskv)(nil)
)

func init() {
	Codec = codec.Gob
}

// Diskv is a Diskv backed key/value store
type Diskv struct {
	dv *diskv.Diskv
}

// New returns a new Diskv backed key/value store
func New(opts diskv.Options) *Diskv {
	return &Diskv{dv: diskv.New(opts)}
}

// Set implements the "kv.Cache".Set() interface
func (d *Diskv) Set(key string, value interface{}) error {
	b, err := Codec.Marshal(value)
	if err != nil {
		return err
	}
	return d.dv.Write(key, b)
}

// Get implements the "kv.Cache".Get() interface
func (d *Diskv) Get(key string, dstVal interface{}) error {
	b, err := d.dv.Read(key)
	if err != nil {
		if strings.HasSuffix(err.Error(), "no such file or directory") {
			err = kv.ErrNotFound
		}
		return err
	}
	return Codec.Unmarshal(b, dstVal)
}

// Del implements the "kv.Cache".Del() interface
func (d *Diskv) Del(key string) error {
	if err := d.Diskv().Erase(key); err != nil {
		if strings.Contains(err.Error(), "no such file or directory") {
			err = kv.ErrNotFound
		}
		return err
	}
	return nil
}

// Exists implements the "kv.Cache".Exists() interface
func (d *Diskv) Exists(key string) bool {
	return d.Diskv().Has(key)
}

// Diskv returns the underlying Diskv struct
func (d *Diskv) Diskv() *diskv.Diskv {
	return d.dv
}
