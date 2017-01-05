GoKV is a package which implements a unified interface for various Golang based key/value stores.

Current drivers include:

- [AppEngine datastore](https://godoc.org/github.com/bradberger/gokv/drivers/appengine/datastore)
- [BoltDB](https://godoc.org/github.com/bradberger/gokv/drivers/boltdb)
- [DiskV](https://godoc.org/github.com/bradberger/gokv/drivers/diskv)
- [LevelDB](https://godoc.org/github.com/bradberger/gokv/drivers/level)

More drivers are most welcome! Just make sure they meet at least the `"kv".Store`
interface and are unit tested.
