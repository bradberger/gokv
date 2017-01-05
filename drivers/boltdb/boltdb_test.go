package boltdb

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/bradberger/gokv/codec"
	"github.com/bradberger/gokv/kv"
	"github.com/stretchr/testify/assert"
)

type testStruct struct {
	Foo string
}

func tmpFile() string {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		panic(err)
	}
	f.Close()
	return f.Name()
}

func TestNew(t *testing.T) {
	fn := tmpFile()
	db, err := New(fn, "test", 0777, nil)
	if !assert.NoError(t, err) {
		return
	}
	defer os.Remove(fn)
	assert.NotNil(t, db)
	assert.NotNil(t, db.db)
	assert.Equal(t, db.db, db.DB())
	assert.Equal(t, "test", db.bucket)
	assert.Equal(t, "test", db.Bucket())
	assert.NoError(t, db.Close())
}

func TestNewErr(t *testing.T) {
	db, err := New("/root", "test", 0777, nil)
	assert.Error(t, err)
	assert.Nil(t, db)
}

func TestSet(t *testing.T) {
	v := testStruct{"bar"}
	fn := tmpFile()
	db, err := New(fn, "test", 0777, nil)
	defer func() {
		db.Close()
		os.Remove(fn)
	}()

	assert.NoError(t, err)
	assert.NoError(t, db.Set("foo", v))
}

func TestSetErr(t *testing.T) {
	v := testStruct{"bar"}
	fn := tmpFile()
	origCodec := Codec
	Codec = codec.ErrTestCodec
	db, err := New(fn, "test", 0777, nil)
	defer func() {
		db.Close()
		os.Remove(fn)
		Codec = origCodec
	}()
	assert.NoError(t, err)
	assert.Error(t, db.Set("foo", v))
}

func TestGet(t *testing.T) {
	v := &testStruct{"bar"}
	vv := &testStruct{}

	fn := tmpFile()
	db, err := New(fn, "test", 0777, nil)
	defer func() {
		db.Close()
		os.Remove(fn)
	}()

	assert.NoError(t, err)
	assert.NoError(t, db.Set("foo", v))
	assert.NoError(t, db.Get("foo", vv))
	assert.EqualValues(t, v, vv)
}

func TestDel(t *testing.T) {
	v := testStruct{"bar"}
	fn := tmpFile()
	db, err := New(fn, "test", 0777, nil)
	defer func() {
		db.Close()
		os.Remove(fn)
	}()

	assert.NoError(t, err)
	assert.NoError(t, db.Set("foo", v))
	assert.NoError(t, db.Del("foo"))
	assert.Equal(t, kv.ErrNotFound, db.Get("foo", &v))
}
