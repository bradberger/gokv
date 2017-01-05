package diskv

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	"github.com/bradberger/gokv/codec"
	"github.com/bradberger/gokv/kv"
	"github.com/peterbourgon/diskv"
	"github.com/stretchr/testify/assert"
)

type testStruct struct {
	Foo, Bar string
}

func getTestOptions() diskv.Options {
	tmpDir, err := ioutil.TempDir("", "diskv")
	if err != nil {
		panic(err)
	}
	return diskv.Options{
		BasePath:     tmpDir,
		Transform:    func(s string) []string { return []string{} },
		CacheSizeMax: 1024 * 1024,
	}
}

func TestNew(t *testing.T) {
	opts := getTestOptions()
	dv := New(opts)
	defer func() {
		os.RemoveAll(opts.BasePath)
	}()
	assert.NotNil(t, dv)
	assert.NotNil(t, dv.dv)
}

func TestSet(t *testing.T) {
	v := &testStruct{"foo", "bar"}
	opts := getTestOptions()
	dv := New(opts)
	defer func() {
		os.RemoveAll(opts.BasePath)
	}()
	assert.NoError(t, dv.Set("foobar", v))
}

func TestSetErr(t *testing.T) {
	v := &testStruct{"foo", "bar"}
	opts := getTestOptions()
	dv := New(opts)
	origCodec := Codec
	Codec = codec.ErrTestCodec
	defer func() {
		Codec = origCodec
		os.RemoveAll(opts.BasePath)
	}()
	assert.Error(t, dv.Set("foobar", v))
}

func TestGet(t *testing.T) {
	v := &testStruct{"foo", "bar"}
	vv := &testStruct{}
	opts := getTestOptions()
	dv := New(opts)
	defer func() {
		os.RemoveAll(opts.BasePath)
	}()
	assert.NoError(t, dv.Set("foobar", v))
	assert.NoError(t, dv.Get("foobar", &vv))
	assert.EqualValues(t, v, vv)
}

func TestGetErr(t *testing.T) {
	var v testStruct
	opts := getTestOptions()
	dv := New(opts)
	assert.Error(t, dv.Get("foobar", &v))
	assert.Equal(t, kv.ErrNotFound, dv.Get("foobar", &v))
}

func TestGetCodecErr(t *testing.T) {
	var v testStruct
	opts := getTestOptions()
	dv := New(opts)
	origCodec := Codec
	Codec = codec.ErrTestCodec
	Codec.Marshal = json.Marshal
	defer func() {
		Codec = origCodec
	}()
	assert.NoError(t, dv.Set("foobar", &v))
	os.RemoveAll(opts.BasePath)
	assert.EqualError(t, dv.Get("foobar", &v), "not found")
}

func TestDel(t *testing.T) {
	v := &testStruct{"foo", "bar"}
	opts := getTestOptions()
	dv := New(opts)
	assert.NoError(t, dv.Set("foobar", &v))
	assert.NoError(t, dv.Del("foobar"))
	assert.False(t, dv.Exists("foobar"))
	assert.Equal(t, kv.ErrNotFound, dv.Del("foobar"))
}
