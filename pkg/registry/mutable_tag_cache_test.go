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
	tagName := "latest"
	img, err := oci.NewImage("example.com", "foo/bar", tagName, staleDigest)
	require.NoError(t, err)
	memStore.AddImage(img)
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

	// Verify the tag was added correctly
	resolvedDigest, err := memStore.Resolve(context.Background(), "example.com/foo/bar:latest")
	require.NoError(t, err)
	require.Equal(t, staleDigest, resolvedDigest)
	t.Logf("Successfully resolved tag to digest: %s", resolvedDigest)

	tests := []struct {
		name                   string
		resolveLatestTag       bool
		disableMutableTagCache bool
		expectedStatus         int
		expectedDigest         string
	}{
		{
			name:                   "Default behavior - Serves stale local digest",
			resolveLatestTag:       true,
			disableMutableTagCache: false,
			expectedStatus:         http.StatusOK,
			expectedDigest:         staleDigest.String(),
		},
		{
			name:                   "DisableMutableTagCache=true - Forces upstream check (404 when no peers)",
			resolveLatestTag:       true,
			disableMutableTagCache: true,
			expectedStatus:         http.StatusNotFound,
			expectedDigest:         "",
		},
		{
			name:                   "ResolveLatestTag=false alone - Still serves local cache (BUG)",
			resolveLatestTag:       false,
			disableMutableTagCache: false,
			expectedStatus:         http.StatusOK,
			expectedDigest:         staleDigest.String(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup Registry
			router := routing.NewMemoryRouter(map[string][]netip.AddrPort{}, netip.AddrPort{})
			reg, err := NewRegistry(
				memStore,
				router,
				WithResolveLatestTag(tt.resolveLatestTag),
				WithDisableMutableTagCache(tt.disableMutableTagCache),
			)
			require.NoError(t, err)

			srv := httptest.NewServer(reg.Handler())
			defer srv.Close()

			// Make a request for the manifest by TAG
			url := fmt.Sprintf("%s/v2/foo/bar/manifests/%s?ns=example.com", srv.URL, tagName)
			t.Logf("Requesting URL: %s", url)
			req, err := http.NewRequest(http.MethodGet, url, nil)
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
