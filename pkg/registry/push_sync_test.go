package registry

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	contentapi "github.com/containerd/containerd/api/services/content/v1"
	"github.com/containerd/containerd/api/services/images/v1"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/plugins/content/local"
	"github.com/containerd/containerd/v2/plugins/services/content/contentserver"
	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/spegel-org/spegel/pkg/oci"
	"github.com/spegel-org/spegel/pkg/routing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestSyncDigestHead(t *testing.T) {
	t.Parallel()
	data := []byte("in Spegel")
	dgst := digest.FromBytes(data)
	for _, tc := range []struct {
		name, mode, endpoint, method, reference string
		marked, local                           bool
		upstreamStatus, status                  int
	}{
		{name: "direct pull", mode: "sync", local: true, status: http.StatusOK},
		{name: "async", mode: "async", marked: true, local: true, status: http.StatusOK},
		{name: "remote marker", mode: "sync", marked: true, status: http.StatusOK},
		{name: "exists upstream", mode: "sync", marked: true, local: true, upstreamStatus: http.StatusOK, status: http.StatusOK},
		{name: "tag lookup", endpoint: "manifests", reference: "v1", mode: "sync", marked: true, local: true, status: http.StatusOK},
		{name: "manifest GET", endpoint: "manifests", method: http.MethodGet, mode: "sync", marked: true, local: true, status: http.StatusOK},
		{name: "manifest exists", endpoint: "manifests", mode: "sync", marked: true, local: true, upstreamStatus: http.StatusOK, status: http.StatusOK},
		{name: "manifest exists only locally", endpoint: "manifests", mode: "sync", marked: true, local: true, upstreamStatus: http.StatusNotFound, status: http.StatusNotFound},
		{name: "exists only locally", mode: "sync", marked: true, local: true, upstreamStatus: http.StatusNotFound, status: http.StatusNotFound},
		{name: "upstream fails", mode: "sync", marked: true, local: true, upstreamStatus: http.StatusServiceUnavailable, status: http.StatusBadGateway},
		{name: "upstream times out", mode: "sync", marked: true, local: true, upstreamStatus: http.StatusGatewayTimeout, status: http.StatusGatewayTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mem := oci.NewMemory()
			mem.Write(ocispec.Descriptor{Digest: dgst, Size: int64(len(data))}, data)
			endpoint := tc.endpoint
			if endpoint == "" {
				endpoint = "blobs"
			}
			reference := tc.reference
			if reference == "" {
				reference = dgst.String()
			} else {
				mem.AddImage(oci.Image{Registry: "registry.fly.io", Repository: "example", Tag: reference, Digest: dgst})
			}
			method := tc.method
			if method == "" {
				method = http.MethodHead
			}
			path := "/v2/example/" + endpoint + "/" + reference
			upstreamCalls := 0
			transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
				upstreamCalls++
				assert.Equal(t, "registry.fly.io", req.URL.Host)
				assert.Equal(t, path, req.URL.Path)
				assert.Empty(t, req.URL.RawQuery)
				assert.Equal(t, "configured", req.Header.Get("Authorization"))
				assert.Empty(t, req.Header.Get(HeaderSpegelPreferUpstream))
				assert.Equal(t, ocispec.MediaTypeImageIndex, req.Header.Get("Accept"))
				if tc.upstreamStatus == http.StatusGatewayTimeout {
					<-req.Context().Done()
					return nil, req.Context().Err()
				}
				return &http.Response{
					Request: req, StatusCode: tc.upstreamStatus, Body: http.NoBody,
					Header: http.Header{"Content-Length": {"50"}},
				}, nil
			})
			reg, err := NewRegistry(mem, nil, WithTransport(transport), WithPushConfig(PushConfig{
				Enabled: true, Upstream: true, UpstreamMode: tc.mode, SyncTimeout: 500 * time.Millisecond,
				UpstreamHeaders: map[string]string{"authorization": "configured"},
			}))
			require.NoError(t, err)
			req := httptest.NewRequestWithContext(t.Context(), method, path+"?ns=registry.fly.io", nil)
			req.Header.Set("Accept", ocispec.MediaTypeImageIndex)
			if tc.marked {
				req.Header.Set(HeaderSpegelPreferUpstream, "true")
			}
			req = req.WithContext(context.WithValue(req.Context(), http.LocalAddrContextKey, &net.TCPAddr{IP: net.IPv6loopback, Port: 5000}))
			if tc.local {
				req.RemoteAddr = "[::1]:1234"
			}
			rec := httptest.NewRecorder()
			reg.Handler().ServeHTTP(rec, req)
			require.Equal(t, tc.status, rec.Code)
			switch tc.upstreamStatus {
			case 0:
				assert.Zero(t, upstreamCalls)
				assert.Empty(t, rec.Header().Get(HeaderSpegelCheckedUpstream))
				assert.Equal(t, strconv.Itoa(len(data)), rec.Header().Get("Content-Length"))
			case http.StatusOK, http.StatusNotFound:
				assert.Equal(t, 1, upstreamCalls)
				assert.Equal(t, "true", rec.Header().Get(HeaderSpegelCheckedUpstream))
				if tc.upstreamStatus == http.StatusOK {
					assert.Equal(t, "50", rec.Header().Get("Content-Length"))
				}
			default:
				assert.Equal(t, 1, upstreamCalls)
				assert.Empty(t, rec.Header().Get(HeaderSpegelCheckedUpstream))
			}
		})
	}
}

func TestUpstreamHeadBearerChallenge(t *testing.T) {
	t.Parallel()
	tokens := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		assert.Equal(t, "repository:example:pull", req.URL.Query().Get("scope"))
		assert.Equal(t, "registry", req.URL.Query().Get("service"))
		assert.Equal(t, "configured", req.Header.Get("X-Registry-Key"))
		if _, err := io.WriteString(w, `{"token":"read-token"}`); err != nil {
			t.Errorf("writing token response: %v", err)
		}
	}))
	t.Cleanup(tokens.Close)
	dist := oci.DistributionPath{
		Kind: oci.DistributionKindBlob, Name: "example", Digest: digest.FromString("layer"),
	}
	var heads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		heads.Add(1)
		assert.Equal(t, http.MethodHead, req.Method)
		assert.Equal(t, "/v2/example/blobs/"+dist.Digest.String(), req.URL.Path)
		if req.Header.Get("Authorization") != "Bearer read-token" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+tokens.URL+`",service="registry"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	dist.Registry = server.Listener.Addr().String()
	reg, err := NewRegistry(nil, nil, WithPushConfig(PushConfig{
		UpstreamHeaders: map[string]string{"X-Registry-Key": "configured"},
	}))
	require.NoError(t, err)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodHead, "/", nil)
	resp, err := reg.upstreamHead(req, dist)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, int32(2), heads.Load())
}

type syncTestImageStore struct {
	images.UnimplementedImagesServer
	created     chan string
	blockCreate <-chan struct{}
}

func (s syncTestImageStore) Create(ctx context.Context, req *images.CreateImageRequest) (*images.CreateImageResponse, error) {
	s.created <- req.Image.Name
	if req.Image.Name == "registry.fly.io/example:concurrent" {
		select {
		case <-s.blockCreate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &images.CreateImageResponse{Image: req.Image}, nil
}

type slowIngester struct {
	content.Ingester
	unavailableOnce bool
}

func (s *slowIngester) Writer(ctx context.Context, opts ...content.WriterOpt) (content.Writer, error) { //nolint:ireturn // Required by content.Ingester.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(200 * time.Millisecond):
	}
	if s.unavailableOnce {
		s.unavailableOnce = false
		return nil, errdefs.ErrUnavailable
	}
	return s.Ingester.Writer(ctx, opts...)
}

func TestSlowWriterOpen(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		unavailableOnce bool
	}{
		{"first open succeeds", false},
		{"slow first open is unavailable and slow retry succeeds", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cs, err := local.NewStore(t.TempDir())
			require.NoError(t, err)
			store := &slowIngester{Ingester: cs, unavailableOnce: tc.unavailableOnce}
			w, err := acquirePushWriter(t.Context(), store, 100*time.Millisecond, content.WithRef("slow-upload"))
			require.NoError(t, err)
			require.NoError(t, w.Close())
		})
	}
}

func TestSyncPushCommits(t *testing.T) {
	t.Parallel()

	cs, err := local.NewStore(t.TempDir())
	require.NoError(t, err)
	sock := filepath.Join(t.TempDir(), "containerd.sock")
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", sock)
	require.NoError(t, err)
	server := grpc.NewServer()
	unblockCreate := make(chan struct{})
	defer close(unblockCreate)
	imageStore := syncTestImageStore{created: make(chan string, 2), blockCreate: unblockCreate}
	contentapi.RegisterContentServer(server, contentserver.New(cs))
	images.RegisterImagesServer(server, imageStore)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		assert.NoError(t, <-serveErr)
	})
	store, err := oci.NewContainerd(sock, "fly", "", nil)
	require.NoError(t, err)
	client, err := store.Client()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, client.Close()) })

	var blobUploads atomic.Int32
	var failCommit atomic.Bool
	var manifestCommits atomic.Int32
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.Method {
		case http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		case http.MethodPost:
			assert.Equal(t, "/v2/example/blobs/uploads/", req.URL.Path)
			w.Header().Set("Location", "/v2/example/blobs/uploads/upstream")
			w.WriteHeader(http.StatusAccepted)
		case http.MethodPut:
			body, err := io.ReadAll(req.Body)
			if !assert.NoError(t, err) {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			dgst := digest.FromBytes(body).String()
			if strings.Contains(req.URL.Path, "/blobs/uploads/") {
				assert.Equal(t, dgst, req.URL.Query().Get("digest"))
				blobUploads.Add(1)
			} else {
				manifestCommits.Add(1)
			}
			if failCommit.Load() {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Docker-Content-Digest", dgst)
			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(upstream.Close)

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		assert.Equal(t, "registry.fly.io", req.URL.Host)
		req = req.Clone(req.Context())
		req.URL.Host = upstream.Listener.Addr().String()
		return upstream.Client().Transport.RoundTrip(req)
	})
	reg, err := NewRegistry(store, routing.NewMemoryRouter(map[string][]netip.AddrPort{}, netip.AddrPort{}), WithTransport(transport), WithPushConfig(PushConfig{
		Enabled: true, Upstream: true, UpstreamMode: "sync",
	}))
	require.NoError(t, err)
	handler := reg.Handler()
	sendWithContext := func(ctx context.Context, method, path, mediaType string, body []byte) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequestWithContext(ctx, method, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", mediaType)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	send := func(method, path, mediaType string, body []byte) *httptest.ResponseRecorder {
		t.Helper()
		return sendWithContext(t.Context(), method, path, mediaType, body)
	}

	blob := []byte("blob to upload")
	dgst := digest.FromBytes(blob).String()
	resp := send(http.MethodPost, "/v2/example/blobs/uploads/?ns=registry.fly.io&digest="+dgst, "", blob)
	require.Equal(t, http.StatusCreated, resp.Code, resp.Body.String())
	require.Equal(t, int32(1), blobUploads.Load())
	require.Equal(t, "/v2/example/blobs/"+dgst, resp.Header().Get("Location"))

	resp = send(http.MethodPost, "/v2/example/blobs/uploads/?ns=registry.fly.io", "", nil)
	require.Equal(t, http.StatusAccepted, resp.Code, resp.Body.String())
	other := []byte("another blob")
	resp = send(http.MethodGet, resp.Header().Get("Location"), "", nil)
	require.Equal(t, http.StatusNoContent, resp.Code, resp.Body.String())
	commitURL, err := url.Parse(resp.Header().Get("Location"))
	require.NoError(t, err)
	query := commitURL.Query()
	query.Set("digest", digest.FromBytes(other).String())
	commitURL.RawQuery = query.Encode()
	// The upload-start RPC can return before containerd releases its writer.
	lockCtx, cancelLock := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelLock()
	lock, err := content.OpenWriter(lockCtx, cs, content.WithRef(uploadRef(resp.Header().Get("Docker-Upload-UUID"))))
	require.NoError(t, err)
	busy := send(http.MethodPut, commitURL.String(), "", other)
	require.NoError(t, lock.Close())
	require.Equal(t, http.StatusServiceUnavailable, busy.Code, busy.Body.String())
	require.Equal(t, int32(1), blobUploads.Load())

	failCommit.Store(true)
	resp = send(http.MethodPut, commitURL.String(), "", other)
	require.Equal(t, http.StatusBadGateway, resp.Code, resp.Body.String())
	require.Equal(t, int32(2), blobUploads.Load())

	manifest := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"digest":"` + dgst + `"},"layers":[]}`)
	resp = send(http.MethodPut, "/v2/example/manifests/latest?ns=registry.fly.io", ocispec.MediaTypeImageManifest, manifest)
	require.Equal(t, http.StatusBadGateway, resp.Code, resp.Body.String())
	require.Empty(t, imageStore.created)

	failCommit.Store(false)
	resp = send(http.MethodPut, commitURL.String(), "", nil)
	require.Equal(t, http.StatusCreated, resp.Code, resp.Body.String())
	require.Equal(t, int32(3), blobUploads.Load())

	manifestDigest := digest.FromBytes(manifest).String()
	resp = send(http.MethodPut, "/v2/example/manifests/"+manifestDigest+"?ns=registry.fly.io", ocispec.MediaTypeImageManifest, manifest)
	require.Equal(t, http.StatusCreated, resp.Code, resp.Body.String())
	require.Len(t, imageStore.created, 1)
	require.Equal(t, "registry.fly.io/example@"+manifestDigest, <-imageStore.created)

	index := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"` + manifestDigest + `"}]}`)
	_, release := reg.tryLockManifestPush("registry.fly.io/example:latest")
	require.NotNil(t, release)
	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	resp = sendWithContext(ctx, http.MethodPut, "/v2/example/manifests/latest?ns=registry.fly.io", ocispec.MediaTypeImageIndex, index)
	require.Equal(t, http.StatusGatewayTimeout, resp.Code, resp.Body.String())
	require.Equal(t, int32(2), manifestCommits.Load())
	require.Empty(t, imageStore.created)

	release()
	resp = send(http.MethodPut, "/v2/example/manifests/latest?ns=registry.fly.io", ocispec.MediaTypeImageIndex, index)
	require.Equal(t, http.StatusCreated, resp.Code, resp.Body.String())
	require.Len(t, imageStore.created, 1)
	require.Equal(t, "registry.fly.io/example:latest", <-imageStore.created)
	// Hold the first request between its upstream PUT and local image creation.
	path := "/v2/example/manifests/concurrent?ns=registry.fly.io"
	first := make(chan *httptest.ResponseRecorder, 1)
	go func() { first <- send(http.MethodPut, path, ocispec.MediaTypeImageIndex, index) }()
	select {
	case name := <-imageStore.created:
		require.Equal(t, "registry.fly.io/example:concurrent", name)
	case <-time.After(5 * time.Second):
		t.Fatal("first PUT did not reach the image store")
	}
	before := manifestCommits.Load()
	waitCtx, stop := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer stop()
	resp = sendWithContext(waitCtx, http.MethodPut, path, ocispec.MediaTypeImageManifest, manifest)
	require.Equal(t, http.StatusGatewayTimeout, resp.Code)
	require.Equal(t, before, manifestCommits.Load())

	resp = send(http.MethodPut, "/v2/example/manifests/independent?ns=registry.fly.io", ocispec.MediaTypeImageManifest, manifest)
	require.Equal(t, http.StatusCreated, resp.Code)
	unblockCreate <- struct{}{}
	require.Equal(t, http.StatusCreated, (<-first).Code)

}
