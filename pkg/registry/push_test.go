package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/metadata"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/plugins/content/local"
	"github.com/go-logr/logr"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/spegel-org/spegel/pkg/routing"
)

// fakeUpstream is a minimal OCI distribution registry that records what is
// pushed to it. A manifest PUT blocks on hold when hold is set.
type fakeUpstream struct {
	manifests     map[string][]byte
	blobs         map[digest.Digest][]byte
	blobUploads   map[digest.Digest]int
	manifestPuts  chan string
	hold          chan struct{}
	failManifests bool
	mu            sync.Mutex
}

func newFakeUpstream() *fakeUpstream {
	return &fakeUpstream{
		manifests:    map[string][]byte{},
		blobs:        map[digest.Digest][]byte{},
		blobUploads:  map[digest.Digest]int{},
		manifestPuts: make(chan string, 16),
	}
}

func (f *fakeUpstream) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	p := req.URL.Path
	switch {
	case strings.Contains(p, "/blobs/uploads/") && req.Method == http.MethodPost:
		rw.Header().Set("Location", "/v2/upload/session")
		rw.WriteHeader(http.StatusAccepted)
	case strings.HasSuffix(p, "/upload/session") && req.Method == http.MethodPut:
		dgst := digest.Digest(req.URL.Query().Get("digest"))
		b, err := io.ReadAll(req.Body)
		if err != nil || dgst.Validate() != nil || digest.FromBytes(b) != dgst {
			rw.WriteHeader(http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.blobs[dgst] = b
		f.blobUploads[dgst]++
		f.mu.Unlock()
		rw.Header().Set("Docker-Content-Digest", dgst.String())
		rw.WriteHeader(http.StatusCreated)
	case strings.Contains(p, "/blobs/"):
		dgst := digest.Digest(p[strings.LastIndex(p, "/")+1:])
		f.mu.Lock()
		b, ok := f.blobs[dgst]
		f.mu.Unlock()
		if !ok {
			rw.WriteHeader(http.StatusNotFound)
			return
		}
		rw.Header().Set("Docker-Content-Digest", dgst.String())
		rw.Header().Set("Content-Length", strconv.Itoa(len(b)))
		rw.WriteHeader(http.StatusOK)
		if req.Method == http.MethodGet {
			if _, err := rw.Write(b); err != nil {
				return
			}
		}
	case strings.Contains(p, "/manifests/") && req.Method == http.MethodPut:
		ref := p[strings.LastIndex(p, "/")+1:]
		b, err := io.ReadAll(req.Body)
		if err != nil {
			rw.WriteHeader(http.StatusBadRequest)
			return
		}
		if f.failManifests {
			rw.WriteHeader(http.StatusForbidden)
			return
		}
		f.manifestPuts <- ref
		if f.hold != nil {
			<-f.hold
		}
		dgst := digest.FromBytes(b)
		f.mu.Lock()
		f.manifests[ref] = b
		f.manifests[dgst.String()] = b
		f.mu.Unlock()
		rw.Header().Set("Docker-Content-Digest", dgst.String())
		rw.WriteHeader(http.StatusCreated)
	case strings.Contains(p, "/manifests/"):
		ref := p[strings.LastIndex(p, "/")+1:]
		f.mu.Lock()
		b, ok := f.manifests[ref]
		f.mu.Unlock()
		if !ok {
			rw.WriteHeader(http.StatusNotFound)
			return
		}
		rw.Header().Set("Docker-Content-Digest", digest.FromBytes(b).String())
		rw.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
		rw.Header().Set("Content-Length", strconv.Itoa(len(b)))
		rw.WriteHeader(http.StatusOK)
		if req.Method == http.MethodGet {
			if _, err := rw.Write(b); err != nil {
				return
			}
		}
	default:
		rw.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeUpstream) hasManifest(ref string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.manifests[ref]
	return ok
}

// newPushRegistry returns a push-enabled registry backed by a real Containerd
// metadata store, and the namespaced context its requests must carry.
func newPushRegistry(t *testing.T, cfg PushConfig) (*Registry, context.Context) {
	t.Helper()

	dir := t.TempDir()
	cs, err := local.NewStore(filepath.Join(dir, "content"))
	require.NoError(t, err)
	bdb, err := bolt.Open(filepath.Join(dir, "meta.db"), 0o600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bdb.Close() })
	db := metadata.NewDB(bdb, cs, nil)
	ctx := namespaces.WithNamespace(t.Context(), "test")
	require.NoError(t, db.Init(ctx))

	cfg.Enabled = true
	router := routing.NewMemoryRouter(map[string][]netip.AddrPort{}, netip.MustParseAddrPort("127.0.0.1:5000"))
	reg, err := NewRegistry(nil, router, WithPushConfig(cfg), WithLogger(logr.Discard()))
	require.NoError(t, err)
	reg.pushStores = &pushBackend{
		content: db.ContentStore(),
		images:  metadata.NewImageStore(db),
		leases:  metadata.NewLeaseManager(db),
	}
	// Runs before the metadata store closes.
	t.Cleanup(reg.pushes.Wait)
	return reg, ctx
}

func doRequest(ctx context.Context, h http.Handler, method, target, contentType string, body []byte) *http.Response {
	rw := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	h.ServeHTTP(rw, req)
	return rw.Result()
}

func pushBlob(t *testing.T, ctx context.Context, h http.Handler, upstream string, b []byte) ocispec.Descriptor {
	t.Helper()

	dgst := digest.FromBytes(b)
	target := "http://localhost/v2/foo/bar/blobs/uploads/?digest=" + dgst.String() + "&ns=" + url.QueryEscape(upstream)
	resp := doRequest(ctx, h, http.MethodPost, target, "", b)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	return ocispec.Descriptor{Digest: dgst, Size: int64(len(b))}
}

func testManifest(t *testing.T, ctx context.Context, h http.Handler, upstream, layer string) []byte {
	t.Helper()

	config := pushBlob(t, ctx, h, upstream, []byte(`{"architecture":"amd64","os":"linux"}`))
	config.MediaType = ocispec.MediaTypeImageConfig
	l := pushBlob(t, ctx, h, upstream, []byte(layer))
	l.MediaType = ocispec.MediaTypeImageLayer
	b, err := json.Marshal(ocispec.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    config,
		Layers:    []ocispec.Descriptor{l},
	})
	require.NoError(t, err)
	return b
}

func (f *fakeUpstream) uploadCounts() map[digest.Digest]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	counts := map[digest.Digest]int{}
	for k, v := range f.blobUploads {
		counts[k] = v
	}
	return counts
}

func TestManifestPutWaitsForUpstreamPush(t *testing.T) {
	t.Parallel()

	upstream := newFakeUpstream()
	upstream.hold = make(chan struct{})
	upstreamSrv := httptest.NewServer(upstream)
	t.Cleanup(upstreamSrv.Close)
	upstreamHost := upstreamSrv.Listener.Addr().String()

	reg, ctx := newPushRegistry(t, PushConfig{Upstream: true, UpstreamWait: true, UpstreamRetries: 1, UpstreamTimeout: time.Minute, LeaseDuration: time.Minute})
	h := reg.Handler()

	manifest := testManifest(t, ctx, h, upstreamHost, "layer")
	respCh := make(chan *http.Response, 1)
	go func() {
		respCh <- doRequest(ctx, h, http.MethodPut, "http://localhost/v2/foo/bar/manifests/latest?ns="+upstreamHost, ocispec.MediaTypeImageManifest, manifest)
	}()

	select {
	case ref := <-upstream.manifestPuts:
		require.Equal(t, "latest", ref)
	case <-time.After(10 * time.Second):
		t.Fatal("upstream never received the manifest")
	}
	select {
	case resp := <-respCh:
		t.Fatalf("manifest upload acknowledged with %d before the upstream stored it", resp.StatusCode)
	case <-time.After(200 * time.Millisecond):
	}
	require.False(t, upstream.hasManifest("latest"))

	close(upstream.hold)
	resp := <-respCh
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, digest.FromBytes(manifest).String(), resp.Header.Get("Docker-Content-Digest"))
	require.True(t, upstream.hasManifest("latest"))
	counts := upstream.uploadCounts()
	require.Len(t, counts, 2)
	for dgst, n := range counts {
		require.Equal(t, 1, n, "blob %s uploaded more than once", dgst)
	}
}

func TestManifestPutAcksBeforeUpstreamPushByDefault(t *testing.T) {
	t.Parallel()

	upstream := newFakeUpstream()
	upstream.hold = make(chan struct{})
	upstreamSrv := httptest.NewServer(upstream)
	t.Cleanup(upstreamSrv.Close)
	upstreamHost := upstreamSrv.Listener.Addr().String()

	reg, ctx := newPushRegistry(t, PushConfig{Upstream: true, UpstreamRetries: 1, UpstreamTimeout: time.Minute, LeaseDuration: time.Minute})
	h := reg.Handler()

	manifest := testManifest(t, ctx, h, upstreamHost, "layer")
	resp := doRequest(ctx, h, http.MethodPut, "http://localhost/v2/foo/bar/manifests/latest?ns="+upstreamHost, ocispec.MediaTypeImageManifest, manifest)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	select {
	case ref := <-upstream.manifestPuts:
		require.Equal(t, "latest", ref)
	case <-time.After(10 * time.Second):
		t.Fatal("upstream never received the manifest")
	}
	close(upstream.hold)
	reg.pushes.Wait()
	require.True(t, upstream.hasManifest("latest"))
}

func TestManifestPutFailsWhenUpstreamPushFails(t *testing.T) {
	t.Parallel()

	upstream := newFakeUpstream()
	upstream.failManifests = true
	upstreamSrv := httptest.NewServer(upstream)
	t.Cleanup(upstreamSrv.Close)
	upstreamHost := upstreamSrv.Listener.Addr().String()

	reg, ctx := newPushRegistry(t, PushConfig{Upstream: true, UpstreamWait: true, UpstreamRetries: 1, UpstreamTimeout: time.Minute})
	h := reg.Handler()

	manifest := testManifest(t, ctx, h, upstreamHost, "layer")
	resp := doRequest(ctx, h, http.MethodPut, "http://localhost/v2/foo/bar/manifests/latest?ns="+upstreamHost, ocispec.MediaTypeImageManifest, manifest)
	require.Equal(t, http.StatusBadGateway, resp.StatusCode)
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(b), `"code":"UNKNOWN"`)
	require.False(t, upstream.hasManifest("latest"))
}

func TestManifestPutWithoutUpstream(t *testing.T) {
	t.Parallel()

	reg, ctx := newPushRegistry(t, PushConfig{LeaseDuration: time.Minute})
	h := reg.Handler()

	manifest := testManifest(t, ctx, h, "upstream.invalid", "layer")
	resp := doRequest(ctx, h, http.MethodPut, "http://localhost/v2/foo/bar/manifests/latest?ns=upstream.invalid", ocispec.MediaTypeImageManifest, manifest)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
}

func TestMultiArchPushUploadsEachBlobOnce(t *testing.T) {
	t.Parallel()

	upstream := newFakeUpstream()
	upstreamSrv := httptest.NewServer(upstream)
	t.Cleanup(upstreamSrv.Close)
	upstreamHost := upstreamSrv.Listener.Addr().String()

	reg, ctx := newPushRegistry(t, PushConfig{Upstream: true, UpstreamWait: true, UpstreamRetries: 1, UpstreamTimeout: time.Minute})
	h := reg.Handler()

	// Clients push each platform manifest by digest, then the index by tag.
	var children []ocispec.Descriptor
	for _, layer := range []string{"amd64 layer", "arm64 layer"} {
		manifest := testManifest(t, ctx, h, upstreamHost, layer)
		dgst := digest.FromBytes(manifest)
		resp := doRequest(ctx, h, http.MethodPut, "http://localhost/v2/foo/bar/manifests/"+dgst.String()+"?ns="+upstreamHost, ocispec.MediaTypeImageManifest, manifest)
		require.Equal(t, http.StatusCreated, resp.StatusCode)
		require.True(t, upstream.hasManifest(dgst.String()))
		children = append(children, ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest, Digest: dgst, Size: int64(len(manifest))})
	}
	index, err := json.Marshal(ocispec.Index{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageIndex, Manifests: children})
	require.NoError(t, err)
	resp := doRequest(ctx, h, http.MethodPut, "http://localhost/v2/foo/bar/manifests/latest?ns="+upstreamHost, ocispec.MediaTypeImageIndex, index)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.True(t, upstream.hasManifest("latest"))

	// The shared config blob and the two layers.
	counts := upstream.uploadCounts()
	require.Len(t, counts, 3)
	for dgst, n := range counts {
		require.Equal(t, 1, n, "blob %s uploaded more than once", dgst)
	}
}

func TestRetryWithBackoffStopsAtDeadline(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	calls := 0
	start := time.Now()
	err := retryWithBackoff(ctx, 10, func() error {
		calls++
		return errors.New("boom")
	}, logr.Discard())
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 1, calls)
	require.Less(t, time.Since(start), time.Second)
}

func TestRetryWithBackoffRunsAtLeastOnce(t *testing.T) {
	t.Parallel()

	calls := 0
	err := retryWithBackoff(t.Context(), 0, func() error {
		calls++
		return nil
	}, logr.Discard())
	require.NoError(t, err)
	require.Equal(t, 1, calls)
}
