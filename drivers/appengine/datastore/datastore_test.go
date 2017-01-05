package datastore

import (
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"golang.org/x/net/context"
	"google.golang.org/appengine/aetest"
	ae "google.golang.org/appengine/datastore"
)

var (
	ctx  context.Context
	done func()
)

type testStruct struct {
	Foo string
}

func TestMain(m *testing.M) {
	var err error
	ctx, done, err = aetest.NewContext()
	if err != nil {
		log.Fatalf("Could not get test context: %v", err)
	}
	code := m.Run()
	done()
	// If stderr/stdout are not closed tests will hang without -test.v
	os.Stdout.Sync()
	os.Stdout.Close()
	os.Stderr.Sync()
	os.Stderr.Close()
	os.Exit(code)
}

func TestKey(t *testing.T) {
	e := New(ctx, "Data")
	assert.EqualValues(t, ae.NewKey(ctx, "Data", "foobar", 0, nil), e.Key("foobar"))
}

func TestGetSetDel(t *testing.T) {
	v := testStruct{"bar"}
	vv := testStruct{}
	e := New(ctx, "Data")
	assert.NoError(t, e.Del("foo"))
	assert.NoError(t, e.Set("foo", &v))
	assert.NoError(t, e.Get("foo", &vv))
	assert.NoError(t, e.Del("foo"))
	assert.EqualValues(t, v, vv)
}

func TestWithContext(t *testing.T) {
	e := &Entity{}
	assert.Equal(t, e, e.WithContext(ctx))
	assert.Equal(t, ctx, e.Context)
}
