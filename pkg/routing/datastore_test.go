package routing

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	dstest "github.com/ipfs/go-datastore/test"
	"github.com/libp2p/go-libp2p-kad-dht/records"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestOrderedDatastoreSuite(t *testing.T) {
	t.Parallel()

	dstest.SubtestAll(t, newOrderedDatastore())
}

func TestOrderedDatastoreQueryPrefix(t *testing.T) {
	t.Parallel()

	keys := []string{
		"/bar",
		"/bar/baz",
		"/bar/baz/qux",
		"/barbaz",
		"/barbaz/qux",
		"/foo/bar",
		"/providers/AB/peer1",
		"/providers/AB/peer2",
		"/providers/ABC/peer1",
		"/providers/A/peer1",
	}
	tests := []struct {
		prefix   string
		expected []string
	}{
		{prefix: "", expected: keys},
		{prefix: "/", expected: keys},
		{prefix: "/bar", expected: []string{"/bar/baz", "/bar/baz/qux"}},
		{prefix: "bar", expected: []string{"/bar/baz", "/bar/baz/qux"}},
		{prefix: "/bar/", expected: []string{"/bar/baz", "/bar/baz/qux"}},
		{prefix: "/ba", expected: []string{}},
		{prefix: "/providers/AB", expected: []string{"/providers/AB/peer1", "/providers/AB/peer2"}},
		{prefix: "/providers", expected: []string{"/providers/A/peer1", "/providers/AB/peer1", "/providers/AB/peer2", "/providers/ABC/peer1"}},
		{prefix: "/missing", expected: []string{}},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("prefix %q", tt.prefix), func(t *testing.T) {
			t.Parallel()

			ordered := newOrderedDatastore()
			naive := ds.NewMapDatastore()
			for _, k := range keys {
				require.NoError(t, ordered.Put(t.Context(), ds.NewKey(k), []byte(k)))
				require.NoError(t, naive.Put(t.Context(), ds.NewKey(k), []byte(k)))
			}
			q := dsq.Query{Prefix: tt.prefix, Orders: []dsq.Order{dsq.OrderByKey{}}}
			orderedKeys := queryKeys(t, ordered, q)
			naiveKeys := queryKeys(t, naive, q)
			require.ElementsMatch(t, tt.expected, orderedKeys)
			require.Equal(t, naiveKeys, orderedKeys)
		})
	}
}

func queryKeys(t *testing.T, d ds.Datastore, q dsq.Query) []string {
	t.Helper()

	res, err := d.Query(t.Context(), q)
	require.NoError(t, err)
	entries, err := res.Rest()
	require.NoError(t, err)
	keys := []string{}
	for _, e := range entries {
		require.Equal(t, e.Key, string(e.Value))
		keys = append(keys, e.Key)
	}
	return keys
}

// BenchmarkProviderLookup measures a provider lookup that misses the provider
// manager's cache, against stores of increasing size. With the map datastore
// the cost grows with the number of records held; with the ordered datastore
// it should not.
func BenchmarkProviderLookup(b *testing.B) {
	stores := map[string]func() ds.Batching{
		"map":     func() ds.Batching { return ds.NewMapDatastore() },
		"ordered": func() ds.Batching { return newOrderedDatastore() },
	}
	for _, name := range []string{"map", "ordered"} {
		for _, n := range []int{10_000, 100_000, 300_000} {
			b.Run(fmt.Sprintf("%s/records=%d", name, n), func(b *testing.B) {
				ctx := context.Background()
				ps, err := pstoremem.NewPeerstore()
				require.NoError(b, err)
				pm, err := records.NewProviderManager(ctx, randomPeerID(b), ps, stores[name]())
				require.NoError(b, err)
				defer pm.Close()

				peers := make([]peer.ID, 100)
				for i := range peers {
					peers[i] = randomPeerID(b)
				}
				keys := make([]mh.Multihash, n/4)
				for i := range keys {
					keys[i] = randomMultihash(b)
				}
				for i := range n {
					err := pm.AddProvider(ctx, keys[i%len(keys)], peer.AddrInfo{ID: peers[i%len(peers)]})
					require.NoError(b, err)
				}

				b.ResetTimer()
				for i := 0; b.Loop(); i++ {
					// Stride through the keys so consecutive lookups miss the
					// provider manager's LRU cache.
					_, err := pm.GetProviders(ctx, keys[(i*7919)%len(keys)])
					require.NoError(b, err)
				}
			})
		}
	}
}

func randomMultihash(tb testing.TB) mh.Multihash {
	tb.Helper()

	buf := make([]byte, 32)
	_, err := rand.Read(buf)
	require.NoError(tb, err)
	h, err := mh.Sum(buf, mh.SHA2_256, -1)
	require.NoError(tb, err)
	return h
}

func randomPeerID(tb testing.TB) peer.ID {
	tb.Helper()

	return peer.ID(randomMultihash(tb))
}
