// Package gokv implements a set of drivers and a common interface for working with different key/value storage systems
package gokv

import (
	"errors"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/bradberger/gokv/kv"
	"stathat.com/c/consistent"
)

const (
	// ReplicateAsync indicates that replication will be done asyncronously.
	// Set commands will return without error as soon as at least one node has
	// the value
	ReplicateAsync ReplicationMethod = iota

	// ReplicateSync indicates that replication will be done syncronously.
	// Set commands will return without error only if all nodes return without error
	ReplicateSync = iota
)

var (
	_ kv.Store = (*Client)(nil)
)

// ReplicationMethod determines whether replication takes place asyncronously or syncronously.
// Use ReplicateAsync for asyncronous replication, ReplicateSync for syncronous replication.
type ReplicationMethod int

// Client is a cache client with built in replication to any number of different caches.
// This allows replication and syncronization across various caches using the set of drivers
// available as subpackages, including Memcached, Redis, in-memory caches, and more.
type Client struct {
	nodes map[string]kv.Store
	ch    *consistent.Consistent

	replicateNodeCt int
	replicateMethod ReplicationMethod

	sync.Mutex
}

// New returns a new initialized cache Client with no nodes.
func New() *Client {
	return &Client{nodes: make(map[string]kv.Store, 0), ch: consistent.New(), replicateMethod: ReplicateAsync}
}

// AddNode adds a cache node with the given name, but only if it doesn't already exist
func (c *Client) AddNode(name string, node kv.Store) error {
	if _, exists := c.nodes[name]; exists {
		return errors.New("node already exists")
	}
	return c.SetNode(name, node)
}

// SetNode sets the cache node with the given name, regardless of whether it already exists or not
func (c *Client) SetNode(name string, node kv.Store) error {
	if node == nil {
		return errors.New("cache node is nil")
	}
	c.nodes[name] = node
	c.ch.Add(name)
	return nil
}

// ReplaceNode adds a cache node with the given name, but only if it already exists
func (c *Client) ReplaceNode(name string, node kv.Store) error {
	if _, exists := c.nodes[name]; !exists {
		return errors.New("node does not exist")
	}
	return c.SetNode(name, node)
}

// RemoveNode removes a node with the given name from the node list
func (c *Client) RemoveNode(name string) error {
	c.Lock()
	defer c.Unlock()
	delete(c.nodes, name)
	c.ch.Remove(name)
	return nil
}

// SetReplicateMethod sets the replication method
func (c *Client) SetReplicateMethod(m ReplicationMethod) {
	c.replicateMethod = m
}

// ReplicateToN sets how many nodes each key should be replicated to
func (c *Client) ReplicateToN(numNodes int) error {
	if numNodes > len(c.ch.Members()) {
		return errors.New("invalid number of nodes")
	}
	c.replicateNodeCt = numNodes
	return nil
}

func (c *Client) node(nodeName string) kv.Store {
	return c.nodes[nodeName]
}

// Set implements the "kv.Store".Set() interface
func (c *Client) Set(key string, value interface{}) (err error) {

	nodes, err := c.ch.GetN(key, c.replicateNodeCt)
	if err != nil {
		return
	}

	if c.replicateMethod == ReplicateSync {
		var eg errgroup.Group
		for i := range nodes {
			nodeName := nodes[i]
			eg.Go(func() error {
				return c.node(nodeName).Set(key, value)
			})
		}
		return eg.Wait()
	}

	err = c.node(nodes[0]).Set(key, value)
	if len(nodes) > 1 {
		nodes = nodes[1:]
		for i := range nodes {
			go c.node(nodes[i]).Set(key, value)
		}
	}

	return
}

// Get implements the "kv.Store".Get() interface. It checks nodes in order
// of priority, and returns success if the value exists on any of them.
func (c *Client) Get(key string, dstVal interface{}) (err error) {
	nodes, err := c.ch.GetN(key, c.replicateNodeCt)
	if err != nil {
		return err
	}
	for i := range nodes {
		if err = c.node(nodes[i]).Get(key, dstVal); err == nil {
			return
		}
	}
	return kv.ErrNotFound
}

// Del implements the "kv.Store".Del() interface. It deletes the given key across
// all replicated nodes and returns error if any of those delete operations fail.
func (c *Client) Del(key string) (err error) {

	nodes, err := c.ch.GetN(key, c.replicateNodeCt)
	if err != nil {
		return
	}

	var eg errgroup.Group
	for i := range nodes {
		name := nodes[i]
		eg.Go(func() error {
			return c.node(name).Del(key)
		})
	}
	return eg.Wait()
}
