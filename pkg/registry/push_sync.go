package registry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/containerd/v2/core/remotes/docker"
	"github.com/containerd/errdefs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/spegel-org/spegel/pkg/httpx"
	"github.com/spegel-org/spegel/pkg/metrics"
	"github.com/spegel-org/spegel/pkg/oci"
)

// HeaderSpegelPreferUpstream requests an upstream existence check when applicable.
const HeaderSpegelPreferUpstream = "X-Spegel-Prefer-Upstream"

// HeaderSpegelCheckedUpstream identifies a confirmed upstream hit or miss.
const HeaderSpegelCheckedUpstream = "X-Spegel-Checked-Upstream"

// Only accept markers from the local host, since the check uses upstream credentials.
func isLocalRequest(req *http.Request) bool {
	local, ok := req.Context().Value(http.LocalAddrContextKey).(*net.TCPAddr)
	if !ok {
		return false
	}
	remote, _, err := net.SplitHostPort(req.RemoteAddr)
	return err == nil && local.IP.Equal(net.ParseIP(remote))
}

func (r *Registry) pushRequest(rw httpx.ResponseWriter, req *http.Request, operation string) (*http.Request, func()) {
	start := time.Now()
	if r.push.mode() != upstreamPushSync {
		return req, func() { r.observePushRequest(rw, operation, start) }
	}
	deadline := start.Add(r.push.SyncTimeout)
	ctx, cancel := context.WithDeadline(req.Context(), deadline)
	controller := http.NewResponseController(rw)
	readDeadlineSet := req.Method != http.MethodHead && controller.SetReadDeadline(deadline) == nil
	return req.WithContext(ctx), func() {
		cancel()
		if readDeadlineSet {
			if err := controller.SetReadDeadline(time.Time{}); err != nil {
				r.log.Error(err, "failed to clear push read deadline")
			}
		}
		r.observePushRequest(rw, operation, start)
	}
}

func (r *Registry) observePushRequest(rw httpx.ResponseWriter, operation string, start time.Time) {
	metrics.PushRequestDuration.WithLabelValues(string(r.push.mode()), operation, strconv.Itoa(rw.Status())).Observe(time.Since(start).Seconds())
}

func (r *Registry) observeUpstreamWait(operation string, start time.Time, err error) {
	result := "success"
	switch {
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded):
		result = "timeout"
	case errors.Is(err, context.Canceled):
		result = "canceled"
	case errdefs.IsNotFound(err) && (operation == "blob_head" || operation == "manifest_head"):
		result = "missing"
	case err != nil:
		result = "failure"
	}
	metrics.UpstreamWaitDuration.WithLabelValues(string(r.push.mode()), operation, result).Observe(time.Since(start).Seconds())
}

func (r *Registry) syncUpstreamHeaders(req *http.Request) http.Header {
	headers := make(http.Header)
	if auth := req.Header.Get("Authorization"); auth != "" {
		headers.Set("Authorization", auth)
	}
	for key, value := range r.push.UpstreamHeaders {
		headers.Set(key, value)
	}
	return headers
}

func (r *Registry) writePushError(rw httpx.ResponseWriter, err error) {
	if errors.Is(err, errWriterRetriesExhausted) {
		rw.WriteError(http.StatusServiceUnavailable, oci.NewDistributionError(oci.ErrCodeUnknown, err.Error(), nil))
		return
	}
	if r.push.mode() == upstreamPushSync && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded)) {
		r.writeUpstreamError(rw, err)
		return
	}
	rw.WriteError(http.StatusInternalServerError, err)
}

func (r *Registry) writeUpstreamError(rw httpx.ResponseWriter, err error) {
	status := http.StatusBadGateway
	message := "upstream registry operation failed"
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		status = http.StatusGatewayTimeout
		message = "upstream registry operation timed out"
	}
	rw.Header().Set(httpx.HeaderContentType, "application/json")
	rw.WriteError(status, errors.Join(oci.NewDistributionError(oci.ErrCodeUnknown, message, nil), err))
}

func (r *Registry) upstreamHead(req *http.Request, dist oci.DistributionPath) (*http.Response, error) {
	headers := r.syncUpstreamHeaders(req)
	hosts, err := r.syncRegistryHosts(headers)(dist.Registry)
	if err != nil {
		return nil, err
	}
	// ConfigureDefaultRegistries returns one canonical host per registry.
	host := hosts[0]
	u := url.URL{
		Scheme: host.Scheme, Host: host.Host,
		Path: path.Join(host.Path, dist.Name, string(dist.Kind), dist.Digest.String()),
	}
	ctx := docker.ContextWithAppendPullRepositoryScope(req.Context(), dist.Name)
	upstreamReq, err := http.NewRequestWithContext(ctx, http.MethodHead, u.String(), nil)
	if err != nil {
		return nil, err
	}
	upstreamReq.Header = headers.Clone()
	if upstreamReq.Header.Get("Accept") == "" {
		for _, value := range req.Header.Values("Accept") {
			upstreamReq.Header.Add("Accept", value)
		}
	}

	resp, err := host.Client.Do(upstreamReq)
	if err != nil || resp.StatusCode != http.StatusUnauthorized {
		return resp, err
	}
	// A redirect target is not the selected registry's authentication authority.
	if resp.Request.URL.Host != u.Host || resp.Request.URL.Scheme != u.Scheme {
		return resp, nil
	}
	resp.Body.Close()
	if err := host.Authorizer.AddResponses(ctx, []*http.Response{resp}); err != nil {
		return nil, err
	}
	retry := upstreamReq.Clone(ctx)
	if err := host.Authorizer.Authorize(ctx, retry); err != nil {
		return nil, err
	}
	return host.Client.Do(retry)
}

func (r *Registry) handleUpstreamHead(rw httpx.ResponseWriter, req *http.Request, dist oci.DistributionPath, operation string) {
	rw.SetHandler("upstreamHead")
	start := time.Now()
	resp, err := r.upstreamHead(req, dist)
	if err != nil {
		r.observeUpstreamWait(operation, start, err)
		r.writeUpstreamError(rw, err)
		return
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		r.observeUpstreamWait(operation, start, nil)
		rw.Header().Set(HeaderSpegelCheckedUpstream, "true")
		rw.Header().Set(oci.HeaderDockerDigest, dist.Digest.String())
		for _, header := range []string{httpx.HeaderContentType, httpx.HeaderContentLength} {
			if value := resp.Header.Get(header); value != "" {
				rw.Header().Set(header, value)
			}
		}
		rw.WriteHeader(http.StatusOK)
	case http.StatusNotFound:
		r.observeUpstreamWait(operation, start, errdefs.ErrNotFound)
		rw.Header().Set(HeaderSpegelCheckedUpstream, "true")
		rw.WriteHeader(http.StatusNotFound)
	default:
		err = httpx.CheckResponseStatus(resp, http.StatusOK, http.StatusNotFound)
		r.observeUpstreamWait(operation, start, err)
		r.writeUpstreamError(rw, err)
	}
}

func (r *Registry) finishBlobUpload(rw httpx.ResponseWriter, req *http.Request, dist oci.DistributionPath, cs content.Store, advertise bool) {
	dist.Kind = oci.DistributionKindBlob
	ctx := context.Background()
	if r.push.mode() == upstreamPushSync {
		ctx = req.Context()
		info, err := cs.Info(ctx, dist.Digest)
		if err != nil {
			r.writePushError(rw, err)
			return
		}
		desc := ocispec.Descriptor{Digest: dist.Digest, Size: info.Size, MediaType: ocispec.MediaTypeImageLayer}
		ref := fmt.Sprintf("%s/%s@%s", dist.Registry, dist.Name, dist.Digest)
		if err = r.pushDescriptor(ctx, req, ref, desc, cs, "blob_commit"); err != nil {
			r.writeUpstreamError(rw, err)
			return
		}
	}
	if advertise {
		r.advertise(ctx, dist.Digest)
	}
	created(rw, dist)
}

// Resumed async pushes and live sync commits share the same reference lock.
func (r *Registry) tryLockManifestPush(ref string) (<-chan struct{}, func()) {
	r.manifestPushMu.Lock()
	defer r.manifestPushMu.Unlock()
	if current := r.activeManifestPushes[ref]; current != nil {
		return current, nil
	}
	done := make(chan struct{})
	r.activeManifestPushes[ref] = done
	return nil, func() {
		r.manifestPushMu.Lock()
		defer r.manifestPushMu.Unlock()
		delete(r.activeManifestPushes, ref)
		close(done)
	}
}

func (r *Registry) lockManifestPush(ctx context.Context, ref string) (func(), error) {
	for ctx.Err() == nil {
		done, release := r.tryLockManifestPush(ref)
		if release != nil {
			return release, nil
		}
		select {
		case <-done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, ctx.Err()
}

func (r *Registry) pushDescriptor(ctx context.Context, req *http.Request, ref string, desc ocispec.Descriptor, cs content.Provider, operation string) (err error) {
	start := time.Now()
	defer func() { r.observeUpstreamWait(operation, start, err) }()
	headers := r.syncUpstreamHeaders(req)
	resolver := docker.NewResolver(docker.ResolverOptions{Hosts: r.syncRegistryHosts(headers), Headers: headers})
	pusher, err := resolver.Pusher(ctx, ref)
	if err != nil {
		return err
	}
	_, err = remotes.PushHandler(pusher, cs)(ctx, desc)
	return err
}

func (r *Registry) syncRegistryHosts(headers http.Header) docker.RegistryHosts {
	authorizer := docker.NewDockerAuthorizer(docker.WithAuthClient(r.upstreamClient), docker.WithAuthHeader(headers))
	return docker.ConfigureDefaultRegistries(
		docker.WithClient(r.upstreamClient),
		docker.WithAuthorizer(authorizer),
		docker.WithPlainHTTP(docker.MatchLocalhost),
	)
}
