package registry

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/spegel-org/spegel/pkg/oci"
	"github.com/spegel-org/spegel/pkg/routing"
	"github.com/stretchr/testify/require"
)

func TestResolveLatestTag_StaleDigest(t *testing.T) {
	t.Parallel()

	// Setup a memory store with a "stale" digest for the "latest" tag.
	memStore := oci.NewMemory()
	staleDigest := digest.Digest("sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	// Pre-populate the store with the stale digest for the tag
	latestTagName := "latest"
	otherTagName := "v1"

	// Add "latest" tag
	img, err := oci.NewImage("example.com", "foo/bar", latestTagName, staleDigest)
	require.NoError(t, err)
	memStore.AddImage(img)

	// Add "v1" tag (same digest)
	imgV1, err := oci.NewImage("example.com", "foo/bar", otherTagName, staleDigest)
	require.NoError(t, err)
	memStore.AddImage(imgV1)
	manifestJSON := []byte(`{
		"schemaVersion": 2,
		"mediaType": "application/vnd.oci.image.manifest.v1+json",
		"config": {
			"mediaType": "application/vnd.oci.image.config.v1+json",
			"digest": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
			"size": 1
		},
		"layers": []
	}`)
	memStore.AddBlob(manifestJSON, staleDigest)

	// Setup a "fresh" digest for the upstream/peer test
	freshDigest := digest.Digest("sha256:9999999999999999999999999999999999999999999999999999999999999999")
	freshManifestJSON := []byte(`{
		"schemaVersion": 2,
		"mediaType": "application/vnd.oci.image.manifest.v1+json",
		"config": {
			"mediaType": "application/vnd.oci.image.config.v1+json",
			"digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
			"size": 1
		},
		"layers": []
	}`)

	// Verify the tags were added correctly
	resolvedDigest, err := memStore.Resolve(context.Background(), "example.com/foo/bar:latest")
	require.NoError(t, err)
	require.Equal(t, staleDigest, resolvedDigest)

	resolvedDigestV1, err := memStore.Resolve(context.Background(), "example.com/foo/bar:v1")
	require.NoError(t, err)
	require.Equal(t, staleDigest, resolvedDigestV1)

	t.Logf("Successfully resolved tags to digest: %s", resolvedDigest)

	tests := []struct {
		setupRouter           func(t *testing.T) (routing.Router, func())
		name                  string
		tagName               string
		expectedDigest        string
		expectedStatus        int
		resolveLatestTag      bool
		disableLatestTagCache bool
	}{
		{
			name:                  "Default behavior - Serves stale local digest",
			tagName:               latestTagName,
			resolveLatestTag:      true,
			disableLatestTagCache: false,
			expectedStatus:        http.StatusOK,
			expectedDigest:        staleDigest.String(),
		},
		{
			name:                  "DisableLatestTagCache=true - Forces upstream check (404 when no peers)",
			tagName:               latestTagName,
			resolveLatestTag:      true,
			disableLatestTagCache: true,
			expectedStatus:        http.StatusNotFound,
			expectedDigest:        "",
		},
		{
			name:                  "DisableLatestTagCache=true - Fetches fresh digest from peer",
			tagName:               latestTagName,
			resolveLatestTag:      true,
			disableLatestTagCache: true,
			expectedStatus:        http.StatusOK,
			expectedDigest:        freshDigest.String(),
			setupRouter: func(t *testing.T) (routing.Router, func()) {
				// Setup a peer registry that has the FRESH digest
				peerStore := oci.NewMemory()
				img, err := oci.NewImage("example.com", "foo/bar", latestTagName, freshDigest)
				require.NoError(t, err)
				peerStore.AddImage(img)
				peerStore.AddBlob(freshManifestJSON, freshDigest)

				// Create peer server
				// We need a router for the peer too, but it doesn't need to know about anything
				peerRouter := routing.NewMemoryRouter(map[string][]netip.AddrPort{}, netip.AddrPort{})
				peerReg, err := NewRegistry(peerStore, peerRouter)
				require.NoError(t, err)
				peerSrv := httptest.NewServer(peerReg.Handler())

				// Setup router pointing to the peer
				peerAddrPort := netip.MustParseAddrPort(peerSrv.Listener.Addr().String())
				router := routing.NewMemoryRouter(map[string][]netip.AddrPort{
					"example.com/foo/bar:latest": {peerAddrPort},
				}, netip.AddrPort{})

				return router, func() {
					peerSrv.Close()
				}
			},
		},
		{
			name:                  "DisableLatestTagCache=true but tag is NOT latest - Should serve local cache",
			tagName:               otherTagName,
			resolveLatestTag:      true,
			disableLatestTagCache: true,
			expectedStatus:        http.StatusOK,
			expectedDigest:        staleDigest.String(),
		},
		{
			name:                  "ResolveLatestTag=false alone - Still serves local cache (BUG)",
			tagName:               latestTagName,
			resolveLatestTag:      false,
			disableLatestTagCache: false,
			expectedStatus:        http.StatusOK,
			expectedDigest:        staleDigest.String(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var router routing.Router
			var cleanup func()
			if tt.setupRouter != nil {
				router, cleanup = tt.setupRouter(t)
				defer cleanup()
			} else {
				router = routing.NewMemoryRouter(map[string][]netip.AddrPort{}, netip.AddrPort{})
			}

			// Setup Registry
			reg, err := NewRegistry(
				memStore,
				router,
				WithResolveLatestTag(tt.resolveLatestTag),
				WithDisableLatestTagCache(tt.disableLatestTagCache),
			)
			require.NoError(t, err)

			srv := httptest.NewServer(reg.Handler())
			defer srv.Close()

			// Make a request for the manifest by TAG
			url := fmt.Sprintf("%s/v2/foo/bar/manifests/%s?ns=example.com", srv.URL, tt.tagName)
			t.Logf("Requesting URL: %s", url)
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
			require.NoError(t, err)

			// The registry handler checks for local content first.
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			t.Logf("Response status: %d", resp.StatusCode)
			require.Equal(t, tt.expectedStatus, resp.StatusCode)

			if tt.expectedStatus == http.StatusOK {
				// Verify we got the stale digest
				dockerDigest := resp.Header.Get(oci.HeaderDockerDigest)
				t.Logf("Docker-Content-Digest header: %s", dockerDigest)
				require.Equal(t, tt.expectedDigest, dockerDigest)
			}
		})
	}
}
