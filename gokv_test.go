package gokv

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	dv "github.com/bradberger/gokv/drivers/diskv"
	"github.com/bradberger/gokv/kv"
	"github.com/peterbourgon/diskv"

	"github.com/stretchr/testify/assert"
)

func getTestOptions() diskv.Options {
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("diskv-%d", rand.Intn(100)))
	if err != nil {
		panic(err)
	}
	return diskv.Options{
		BasePath:     tmpDir,
		Transform:    func(s string) []string { return []string{} },
		CacheSizeMax: 1024 * 1024,
	}
}

func TestReplicationN(t *testing.T) {

	opts := getTestOptions()
	opts2 := getTestOptions()
	db := dv.New(opts)
	db2 := dv.New(opts2)
	defer func() {
		os.RemoveAll(opts.BasePath)
		os.RemoveAll(opts2.BasePath)
	}()

	var s string
	c := New()

	assert.Error(t, c.Set("foo", "bar"))

	c.AddNode("node-01", db)
	c.ReplicateToN(2)
	assert.NoError(t, c.Set("foo", "bar"))

	c.AddNode("node-02", db2)
	assert.NoError(t, c.Set("foo", "bar"))

	assert.NoError(t, c.nodes["node-01"].Get("foo", &s))
	assert.NoError(t, c.nodes["node-02"].Get("foo", &s))
}

func TestSetNodes(t *testing.T) {
	c := New()

	opts := getTestOptions()
	db := dv.New(opts)
	defer func() {
		os.RemoveAll(opts.BasePath)
	}()

	assert.Error(t, c.ReplaceNode("node-01", db))
	assert.Error(t, c.SetNode("node-01", nil))
	assert.NoError(t, c.AddNode("node-01", db))
	assert.NoError(t, c.ReplaceNode("node-01", db))
	assert.Error(t, c.AddNode("node-01", db))
	assert.NoError(t, c.RemoveNode("node-01"))
	assert.Len(t, c.nodes, 0)
}

func TestGetErr(t *testing.T) {

	opts := getTestOptions()
	db := dv.New(opts)
	defer func() {
		os.RemoveAll(opts.BasePath)
	}()

	var v string
	c := New()
	c.AddNode("node-01", db)
	assert.Equal(t, kv.ErrNotFound, c.Get("foo", &v))
}

func TestReplicationSync(t *testing.T) {

	opts := getTestOptions()
	opts2 := getTestOptions()
	db := dv.New(opts)
	db2 := dv.New(opts2)
	defer func() {
		os.RemoveAll(opts.BasePath)
		os.RemoveAll(opts2.BasePath)
	}()

	assert.NotEqual(t, opts.BasePath, opts2.BasePath)

	var s string
	c := New()
	c.AddNode("node-01", db)
	c.AddNode("node-02", db2)
	c.ReplicateToN(2)
	c.SetReplicateMethod(ReplicateSync)

	assert.NoError(t, c.Set("foo", "bar"))
	assert.NoError(t, c.nodes["node-01"].Get("foo", &s))
	assert.NoError(t, c.nodes["node-02"].Get("foo", &s))
	assert.NoError(t, c.nodes["node-01"].Del("foo"))
	assert.NoError(t, c.Get("foo", &s))
	assert.Equal(t, "bar", s)
	assert.True(t, db2.Diskv().Has("foo"))
	assert.NoError(t, c.nodes["node-02"].Del("foo"))
	assert.Equal(t, kv.ErrNotFound, c.Del("foo"))
}

func TestGetNErr(t *testing.T) {
	c := New()
	assert.Error(t, c.Del("foo"))
	assert.Error(t, c.Set("foo", "bar"))
	assert.Error(t, c.Get("foo", nil))
}
