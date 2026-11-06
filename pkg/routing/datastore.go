package routing

import (
	"context"
	"path"
	"strings"
	"sync"

	"github.com/google/btree"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

var _ ds.Batching = &orderedDatastore{}

type orderedEntry struct {
	key   string
	value []byte
}

func lessOrderedEntry(a, b orderedEntry) bool {
	return a.key < b.key
}

// orderedDatastore is an in-memory datastore that keeps its keys sorted, so a
// prefix query visits only the keys under that prefix.
//
// The DHT stores provider records under /providers/<key>/<peer> and reads
// them back with a prefix query for each lookup. The DHT's default in-memory
// datastore answers that query by copying and filtering every record it holds,
// and provider records are kept for days, so every lookup gets slower the
// longer the process runs. Here a lookup costs O(log n) plus the records for
// the one key.
type orderedDatastore struct {
	tree *btree.BTreeG[orderedEntry]
	mx   sync.RWMutex
}

func newOrderedDatastore() *orderedDatastore {
	return &orderedDatastore{
		tree: btree.NewG(32, lessOrderedEntry),
	}
}

func (d *orderedDatastore) Put(ctx context.Context, key ds.Key, value []byte) error {
	d.mx.Lock()
	defer d.mx.Unlock()
	d.tree.ReplaceOrInsert(orderedEntry{key: key.String(), value: value})
	return nil
}

func (d *orderedDatastore) Sync(ctx context.Context, prefix ds.Key) error {
	return nil
}

func (d *orderedDatastore) Get(ctx context.Context, key ds.Key) ([]byte, error) {
	d.mx.RLock()
	defer d.mx.RUnlock()
	e, ok := d.tree.Get(orderedEntry{key: key.String()})
	if !ok {
		return nil, ds.ErrNotFound
	}
	return e.value, nil
}

func (d *orderedDatastore) Has(ctx context.Context, key ds.Key) (bool, error) {
	d.mx.RLock()
	defer d.mx.RUnlock()
	return d.tree.Has(orderedEntry{key: key.String()}), nil
}

func (d *orderedDatastore) GetSize(ctx context.Context, key ds.Key) (int, error) {
	d.mx.RLock()
	defer d.mx.RUnlock()
	e, ok := d.tree.Get(orderedEntry{key: key.String()})
	if !ok {
		return -1, ds.ErrNotFound
	}
	return len(e.value), nil
}

func (d *orderedDatastore) Delete(ctx context.Context, key ds.Key) error {
	d.mx.Lock()
	defer d.mx.Unlock()
	d.tree.Delete(orderedEntry{key: key.String()})
	return nil
}

// Query collects the entries under the query prefix while holding the read
// lock, then applies the rest of the query to that snapshot.
func (d *orderedDatastore) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	prefix := queryPrefix(q.Prefix)
	entries := []dsq.Entry{}
	collect := func(e orderedEntry) bool {
		if !strings.HasPrefix(e.key, prefix) {
			return false
		}
		entry := dsq.Entry{Key: e.key, Size: len(e.value)}
		if !q.KeysOnly {
			entry.Value = e.value
		}
		entries = append(entries, entry)
		return true
	}
	d.mx.RLock()
	if prefix == "" {
		d.tree.Ascend(collect)
	} else {
		d.tree.AscendGreaterOrEqual(orderedEntry{key: prefix}, collect)
	}
	d.mx.RUnlock()

	// The prefix is already applied; NaiveQueryApply handles filters, orders,
	// offset and limit. The results still report the query as asked.
	r := dsq.ResultsWithEntries(q, entries)
	rest := q
	rest.Prefix = ""
	return dsq.NaiveQueryApply(rest, r), nil
}

func (d *orderedDatastore) Batch(ctx context.Context) (ds.Batch, error) {
	return ds.NewBasicBatch(d), nil
}

func (d *orderedDatastore) Close() error {
	return nil
}

// queryPrefix returns the key prefix a query matches, following the same
// rules as the datastore's naive query: the prefix is cleaned as a key and
// matches only keys below it, so /bar finds /bar/baz but not /barbaz. An
// empty result matches every key.
func queryPrefix(prefix string) string {
	if prefix == "" {
		return ""
	}
	if prefix[0] != '/' {
		prefix = "/" + prefix
	}
	prefix = path.Clean(prefix)
	if prefix == "/" {
		return ""
	}
	return prefix + "/"
}
