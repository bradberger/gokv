package leveldb

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

func tmpDir() string {
	tmpDir, err := ioutil.TempDir("", "leveldb")
	if err != nil {
		panic(err)
	}
	return tmpDir
}

func TestNew(t *testing.T) {
	dir := tmpDir()
	db, err := New(dir, nil)
	defer func() {
		db.Close()
		os.RemoveAll(dir)
	}()
	assert.NoError(t, err)
	assert.NotNil(t, db.DB())
	assert.Equal(t, db.db, db.DB())
}

func TestNewErr(t *testing.T) {
	db, err := New("/root", nil)
	assert.Error(t, err)
	assert.Nil(t, db)
}

func TestSet(t *testing.T) {
	v := testStruct{"bar"}
	dir := tmpDir()
	db, err := New(dir, nil)
	defer func() {
		db.Close()
		os.RemoveAll(dir)
	}()
	assert.NoError(t, err)
	assert.NoError(t, db.Set("foo", &v))
}

func TestSetErr(t *testing.T) {
	v := testStruct{"bar"}
	dir := tmpDir()
	db, err := New(dir, nil)
	origCodec := Codec
	Codec = codec.ErrTestCodec
	defer func() {
		db.Close()
		os.RemoveAll(dir)
		Codec = origCodec
	}()
	assert.NoError(t, err)
	assert.Error(t, db.Set("foo", v))
}

func TestGet(t *testing.T) {
	v := testStruct{"bar"}
	vv := testStruct{}
	dir := tmpDir()
	db, err := New(dir, nil)
	defer func() {
		db.Close()
		os.RemoveAll(dir)
	}()
	assert.NoError(t, err)
	assert.NoError(t, db.Set("foo", &v))
	assert.NoError(t, db.Get("foo", &vv))
	assert.EqualValues(t, v, vv)
}

func TestDel(t *testing.T) {
	v := testStruct{"bar"}
	vv := testStruct{}
	dir := tmpDir()
	db, err := New(dir, nil)
	defer func() {
		db.Close()
		os.RemoveAll(dir)
	}()
	assert.NoError(t, err)
	assert.NoError(t, db.Set("foo", &v))
	assert.NoError(t, db.Del("foo"))
	assert.Equal(t, kv.ErrNotFound, db.Get("foo", &vv))
}
