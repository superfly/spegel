package registry

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/containerd/v2/core/remotes/docker"
	"github.com/containerd/containerd/v2/pkg/labels"
	"github.com/containerd/errdefs"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/spegel-org/spegel/pkg/httpx"
	"github.com/spegel-org/spegel/pkg/oci"
)

type PushConfig struct {
	UpstreamHeaders map[string]string `arg:"--push-upstream-headers,env:PUSH_UPSTREAM_HEADERS" help:"HTTP header overrides for upstream push request."`
	LeaseDuration   time.Duration     `arg:"--push-lease-duration,env:PUSH_LEASE_DURATION" default:"10m" help:"Temporary lease duration for pushed images."`
	UpstreamTimeout time.Duration     `arg:"--push-upstream-timeout,env:PUSH_UPSTREAM_TIMEOUT" default:"15m" help:"Total time allowed for pushing an image to the upstream registry, all attempts included. With --push-upstream-wait, keep it below the response timeout of any proxy in front of Spegel."`
	UpstreamRetries int               `arg:"--push-upstream-retries,env:PUSH_UPSTREAM_RETRIES" default:"10" help:"Number of attempts for an upstream image push."`
	Enabled         bool              `arg:"--push,env:PUSH" default:"false" help:"When true handles push endpoints by writing to the Containerd content and image store."`
	Upstream        bool              `arg:"--push-upstream,env:PUSH_UPSTREAM" default:"false" help:"When true pushes uploaded blobs and images to the upstream registry."`
	UpstreamWait    bool              `arg:"--push-upstream-wait,env:PUSH_UPSTREAM_WAIT" default:"false" help:"When true acknowledges a manifest upload only once its image is in the upstream registry, and fails the upload when the upstream push fails."`
}

// pushBackend is the Containerd state the push endpoints write to.
type pushBackend struct {
	content content.Store
	images  images.Store
	leases  leases.Manager
}

func (r *Registry) getPushBackend() (pushBackend, error) {
	if r.pushStores != nil {
		return *r.pushStores, nil
	}
	cdc, err := r.getContainerdClient()
	if err != nil {
		return pushBackend{}, err
	}
	return pushBackend{
		content: cdc.ContentStore(),
		images:  cdc.ImageService(),
		leases:  cdc.LeasesService(),
	}, nil
}

// Add a temporary lease to newly created content and images.
func (r *Registry) withLease(ctx context.Context) (context.Context, error) {
	if r.push.LeaseDuration == 0 {
		return ctx, nil
	}
	backend, err := r.getPushBackend()
	if err != nil {
		return nil, err
	}
	l, err := backend.leases.Create(ctx, leases.WithRandomID(), leases.WithExpiration(r.push.LeaseDuration))
	if err != nil {
		return nil, fmt.Errorf("failed to create lease: %w", err)
	}
	return leases.WithLease(ctx, l.ID), nil
}

// createSessionLease creates a lease keyed off the upload session and returns
// a leased ctx.
func (r *Registry) createSessionLease(ctx context.Context, session string, dist oci.DistributionPath) (context.Context, error) {
	if r.push.LeaseDuration == 0 {
		return ctx, nil
	}
	backend, err := r.getPushBackend()
	if err != nil {
		return nil, err
	}
	id := uploadLeaseID(session)
	_, err = backend.leases.Create(ctx,
		leases.WithID(id),
		leases.WithExpiration(r.push.LeaseDuration),
		leases.WithLabels(map[string]string{
			"spegel.org/upload-registry": dist.Registry,
			"spegel.org/upload-name":     dist.Name,
		}))
	if err != nil {
		return nil, fmt.Errorf("failed to create session lease: %w", err)
	}
	return leases.WithLease(ctx, id), nil
}

// withSessionLease attaches an existing session lease to ctx when leasing is enabled.
func (r *Registry) withSessionLease(ctx context.Context, session string) context.Context {
	if r.push.LeaseDuration == 0 {
		return ctx
	}
	return leases.WithLease(ctx, uploadLeaseID(session))
}

// deleteSessionLease removes the lease for this session. Used on failure paths
// to trigger lease-driven ingest GC immediately instead of waiting for expiry.
func (r *Registry) deleteSessionLease(session string) {
	if r.push.LeaseDuration == 0 {
		return
	}
	backend, err := r.getPushBackend()
	if err != nil {
		r.log.Error(err, "failed to get containerd client for lease delete")
		return
	}
	// Use background context so cleanup runs when the request was cancelled.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := backend.leases.Delete(ctx, leases.Lease{ID: uploadLeaseID(session)}); err != nil && !errdefs.IsNotFound(err) {
		r.log.Error(err, "failed to delete session lease", "session", session)
	}
}

func uploadRef(id string) string {
	return ("spegel-upload:") + id
}

func uploadLeaseID(session string) string {
	return "spegel-upload-" + session
}

func withSource(dist oci.DistributionPath) content.Opt {
	return content.WithLabels(map[string]string{labels.LabelDistributionSource + "." + dist.Registry: dist.Name})
}

func uploadStatus(rw httpx.ResponseWriter, dist oci.DistributionPath, offset int64) {
	rw.Header().Set("Location", "/v2/"+dist.Name+"/blobs/uploads/"+dist.Session)
	rw.Header().Set("Docker-Upload-UUID", dist.Session)
	rw.Header().Set("Range", "0-"+strconv.FormatInt(max(0, offset-1), 10))
	rw.Header().Set(httpx.HeaderContentLength, "0")
}

func created(rw httpx.ResponseWriter, dist oci.DistributionPath) {
	rw.Header().Set(oci.HeaderDockerDigest, dist.Digest.String())
	rw.Header().Set("Location", dist.URL().Path)
	rw.Header().Set(httpx.HeaderContentLength, "0")
	rw.WriteHeader(http.StatusCreated)
}

func (r *Registry) pushHandler(rw httpx.ResponseWriter, req *http.Request) {
	rw.SetHandler("push")

	if !r.push.Enabled {
		rw.WriteError(http.StatusMethodNotAllowed, oci.NewDistributionError(oci.ErrCodeUnsupported, "push endpoints disabled", nil))
		return
	}

	// Check basic authentication
	if r.username != "" || r.password != "" {
		username, password, _ := req.BasicAuth()
		if r.username != username || r.password != password {
			rw.WriteError(http.StatusUnauthorized, oci.NewDistributionError(oci.ErrCodeUnauthorized, "invalid credentials", nil))
			return
		}
	}

	// Parse out path components from request.
	dist, err := oci.ParseDistributionPath(req.URL)
	if err != nil {
		rw.WriteError(http.StatusNotFound, fmt.Errorf("could not parse path according to OCI distribution spec: %w", err))
		return
	}

	backend, err := r.getPushBackend()
	if err != nil {
		rw.WriteError(http.StatusMethodNotAllowed, oci.NewDistributionError(oci.ErrCodeUnsupported, err.Error(), nil))
		return
	}
	cs := backend.content

	if dist.Kind == oci.DistributionKindUpload {
		if req.Method == http.MethodPost && dist.Session == "" {
			if string(dist.Digest) == "" {
				r.handleBlobUploadStart(rw, req, dist, cs)
			} else {
				r.handleBlobUploadMonolithic(rw, req, dist, cs)
			}
			return
		}
		if dist.Session != "" {
			switch req.Method {
			case http.MethodPatch:
				r.handleBlobUploadChunk(rw, req, dist, cs)
				return
			case http.MethodPut:
				r.handleBlobUploadCommit(rw, req, dist, cs)
				return
			case http.MethodGet:
				r.handleBlobUploadGet(rw, req, dist, cs)
				return
			}
		}

	}

	if dist.Kind == oci.DistributionKindManifest && req.Method == http.MethodPut {
		r.handleManifestPut(rw, req, dist, backend)
		return
	}

	rw.WriteError(http.StatusNotFound, oci.NewDistributionError(oci.ErrCodeUnsupported, "unsupported push endpoint", nil))
}

func (r *Registry) getContainerdClient() (*client.Client, error) {
	cd, ok := r.ociStore.(*oci.Containerd)
	if !ok {
		return nil, errors.New("push requires containerd store")
	}
	return cd.Client()
}

func (r *Registry) handleBlobUploadMonolithic(rw httpx.ResponseWriter, req *http.Request, dist oci.DistributionPath, cs content.Store) {
	if err := dist.Digest.Validate(); err != nil {
		rw.WriteError(http.StatusBadRequest, oci.NewDistributionError(oci.ErrCodeDigestInvalid, "invalid digest", err.Error()))
		return
	}
	session := uuid.NewString()
	ctx, err := r.createSessionLease(req.Context(), session, dist)
	if err != nil {
		rw.WriteError(http.StatusInternalServerError, err)
		return
	}
	success := false
	defer func() {
		if !success {
			r.deleteSessionLease(session)
		}
	}()

	w, err := cs.Writer(ctx, content.WithRef(uploadRef(session)))
	if err != nil {
		rw.WriteError(http.StatusInternalServerError, err)
		return
	}
	defer w.Close()

	n, err := io.Copy(w, req.Body)
	if err != nil {
		rw.WriteError(http.StatusInternalServerError, err)
		return
	}
	if err = w.Commit(ctx, n, dist.Digest, withSource(dist)); err != nil && !errdefs.IsAlreadyExists(err) {
		rw.WriteError(http.StatusInternalServerError, err)
		return
	}
	success = true
	r.advertise(dist.Digest)
	r.pushBlobUpstream(ctx, req, dist, cs, n)

	created(rw, dist)
}

func (r *Registry) handleBlobUploadStart(rw httpx.ResponseWriter, req *http.Request, dist oci.DistributionPath, cs content.Store) {
	dist.Session = uuid.NewString()
	ctx, err := r.createSessionLease(req.Context(), dist.Session, dist)
	if err != nil {
		rw.WriteError(http.StatusInternalServerError, err)
		return
	}
	w, err := cs.Writer(ctx, content.WithRef(uploadRef(dist.Session)))
	if err != nil {
		r.deleteSessionLease(dist.Session)
		rw.WriteError(http.StatusInternalServerError, err)
		return
	}
	_ = w.Close()
	uploadStatus(rw, dist, 0)
	rw.WriteHeader(http.StatusAccepted)
}

func (r *Registry) handleBlobUploadChunk(rw httpx.ResponseWriter, req *http.Request, dist oci.DistributionPath, cs content.Store) {
	ctx := r.withSessionLease(req.Context(), dist.Session)

	w, err := cs.Writer(ctx, content.WithRef(uploadRef(dist.Session)))
	if errdefs.IsNotFound(err) {
		rw.WriteError(http.StatusNotFound, oci.NewDistributionError(oci.ErrCodeBlobUploadUnknown, "unknown upload session", nil))
		return
	}
	if err != nil {
		rw.WriteError(http.StatusInternalServerError, err)
		return
	}
	defer w.Close()

	if _, err = io.Copy(w, req.Body); err != nil {
		rw.WriteError(http.StatusInternalServerError, err)
		return
	}

	status, err := w.Status()
	if err != nil {
		rw.WriteError(http.StatusInternalServerError, err)
		return
	}

	uploadStatus(rw, dist, status.Offset)
	rw.WriteHeader(http.StatusAccepted)
}

func (r *Registry) handleBlobUploadCommit(rw httpx.ResponseWriter, req *http.Request, dist oci.DistributionPath, cs content.Store) {
	if err := dist.Digest.Validate(); err != nil {
		rw.WriteError(http.StatusBadRequest, oci.NewDistributionError(oci.ErrCodeDigestInvalid, "invalid digest", err.Error()))
		return
	}
	ctx := r.withSessionLease(req.Context(), dist.Session)
	success := false
	defer func() {
		if !success {
			r.deleteSessionLease(dist.Session)
		}
	}()

	desc := ocispec.Descriptor{Digest: dist.Digest}
	w, err := cs.Writer(ctx, content.WithRef(uploadRef(dist.Session)), content.WithDescriptor(desc))
	if errdefs.IsAlreadyExists(err) {
		// Another concurrent upload may have already provided this content digest.
		// containerd has attached the existing blob to our session lease, so keep
		// the lease alive.
		success = true
		dist.Kind = oci.DistributionKindBlob
		created(rw, dist)
		return
	}
	if errdefs.IsNotFound(err) {
		rw.WriteError(http.StatusNotFound, oci.NewDistributionError(oci.ErrCodeBlobUploadUnknown, "unknown upload session", nil))
		return
	}
	if err != nil {
		rw.WriteError(http.StatusInternalServerError, err)
		return
	}
	defer w.Close()

	// final chunk
	if _, err = io.Copy(w, req.Body); err != nil {
		rw.WriteError(http.StatusInternalServerError, err)
		return
	}
	status, err := w.Status()
	if err != nil {
		rw.WriteError(http.StatusInternalServerError, err)
		return
	}

	if err = w.Commit(ctx, status.Offset, dist.Digest, withSource(dist)); err != nil && !errdefs.IsAlreadyExists(err) {
		rw.WriteError(http.StatusInternalServerError, err)
		return
	}
	success = true
	r.advertise(dist.Digest)
	r.pushBlobUpstream(ctx, req, dist, cs, status.Offset)

	dist.Kind = oci.DistributionKindBlob
	created(rw, dist)
}

func (r *Registry) handleBlobUploadGet(rw httpx.ResponseWriter, req *http.Request, dist oci.DistributionPath, cs content.Store) {
	status, err := cs.Status(req.Context(), uploadRef(dist.Session))
	if err != nil && errdefs.IsNotFound(err) {
		rw.WriteError(http.StatusNotFound, oci.NewDistributionError(oci.ErrCodeBlobUploadUnknown, "unknown upload session", nil))
		return
	} else if err != nil {
		rw.WriteError(http.StatusInternalServerError, err)
		return
	}

	uploadStatus(rw, dist, status.Offset)
	rw.WriteHeader(http.StatusNoContent)
}

func (r *Registry) handleManifestPut(rw httpx.ResponseWriter, req *http.Request, dist oci.DistributionPath, backend pushBackend) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		rw.WriteError(http.StatusInternalServerError, err)
		return
	}
	mediaType := req.Header.Get(httpx.HeaderContentType)
	if mediaType == "" {
		mediaType, err = oci.DetermineMediaType(body)
		if err != nil {
			rw.WriteError(http.StatusBadRequest, oci.NewDistributionError(oci.ErrCodeManifestInvalid, "cannot determine manifest media type", err.Error()))
			return
		}
	}
	size := int64(len(body))
	desc := ocispec.Descriptor{MediaType: mediaType, Digest: digest.FromBytes(body), Size: size}

	ctx, err := r.withLease(req.Context())
	if err != nil {
		rw.WriteError(http.StatusInternalServerError, err)
		return
	}

	cs := backend.content
	w, err := cs.Writer(ctx, content.WithRef(dist.Reference()))
	if err != nil && !errdefs.IsAlreadyExists(err) {
		rw.WriteError(http.StatusInternalServerError, err)
		return
	}
	if err == nil {
		defer w.Close()
		if _, err := io.Copy(w, bytes.NewReader(body)); err != nil {
			rw.WriteError(http.StatusInternalServerError, err)
			return
		}
		if err = w.Commit(ctx, size, desc.Digest, withSource(dist)); err != nil && !errdefs.IsAlreadyExists(err) {
			rw.WriteError(http.StatusInternalServerError, err)
			return
		}
	}

	ref := dist.Reference()
	if dist.Digest != "" {
		ref = fmt.Sprintf("%s/%s@%s", dist.Registry, dist.Name, desc.Digest)
	}
	image := images.Image{Name: ref, Target: desc}

	imageService := backend.images
	_, err = imageService.Create(ctx, image)
	if err != nil && errdefs.IsAlreadyExists(err) {
		_, err = imageService.Update(ctx, image)
	}
	if err != nil {
		rw.WriteError(http.StatusInternalServerError, err)
		return
	}

	if err = images.Dispatch(ctx, images.SetChildrenLabels(cs, images.ChildrenHandler(cs)), nil, desc); err != nil {
		rw.WriteError(http.StatusInternalServerError, fmt.Errorf("failed to set image labels: %w", err))
		return
	}

	if r.push.Upstream {
		pushHeaders := upstreamHeaders(req, r.push.UpstreamHeaders)
		repo := dist.Registry + "/" + dist.Name
		// A client that disconnects does not cancel the push: the upstream copy is
		// still wanted, and a retried push finds the content already in place.
		pushCtx := context.WithoutCancel(ctx)
		if !r.push.UpstreamWait {
			r.pushes.Add(1)
			go func() {
				defer r.pushes.Done()
				//nolint:errcheck // pushUpstream logs its own failure.
				r.pushUpstream(pushCtx, repo, ref, desc, cs, pushHeaders)
			}()
		} else if err := r.pushUpstream(pushCtx, repo, ref, desc, cs, pushHeaders); err != nil {
			// The client resolves the reference from the upstream registry once its
			// push succeeds, so the upload only succeeds once the image is there.
			rw.WriteError(http.StatusBadGateway, oci.NewDistributionError(oci.ErrCodeUnknown, "failed to push image to upstream registry", err.Error()))
			return
		}
	}

	dist.Digest = desc.Digest
	created(rw, dist)
}

func upstreamHeaders(req *http.Request, overrides map[string]string) http.Header {
	h := req.Header.Clone()
	for k, v := range overrides {
		h.Set(k, v)
	}
	return h
}

// pushBlobUpstream starts copying a blob to the upstream registry as soon as
// its upload completes, so the upstream push overlaps the client's own push
// and little is left to upload when the manifest arrives. It makes a single
// attempt: the manifest push uploads whatever is still missing.
func (r *Registry) pushBlobUpstream(ctx context.Context, req *http.Request, dist oci.DistributionPath, cs content.Store, size int64) {
	if !r.push.Upstream {
		return
	}
	repo := dist.Registry + "/" + dist.Name
	ref := repo + "@" + dist.Digest.String()
	desc := ocispec.Descriptor{MediaType: "application/octet-stream", Digest: dist.Digest, Size: size}
	headers := upstreamHeaders(req, r.push.UpstreamHeaders)
	done := r.blobPushes.start(repo)
	ctx = context.WithoutCancel(ctx)
	r.pushes.Add(1)
	go func() {
		defer r.pushes.Done()
		defer done()
		if r.push.UpstreamTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, r.push.UpstreamTimeout)
			defer cancel()
		}
		log := r.log.WithName("upstreamPush").WithValues("ref", ref)
		start := time.Now()
		pusher, err := docker.NewResolver(docker.ResolverOptions{Headers: headers}).Pusher(ctx, ref)
		if err == nil {
			err = remotes.PushContent(ctx, pusher, desc, cs, nil, nil, nil)
		}
		if err != nil {
			log.Error(err, "Failed to push blob upstream", "duration", time.Since(start))
			return
		}
		log.V(4).Info("Pushed blob upstream", "duration", time.Since(start))
	}()
}

// blobPushTracker counts the blob pushes in flight per repository, so an
// image push can wait for them rather than upload the same blobs again.
type blobPushTracker struct {
	repos map[string]*blobPushes
	mu    sync.Mutex
}

type blobPushes struct {
	done chan struct{}
	n    int
}

func (t *blobPushTracker) start(repo string) func() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.repos == nil {
		t.repos = map[string]*blobPushes{}
	}
	p, ok := t.repos[repo]
	if !ok {
		p = &blobPushes{done: make(chan struct{})}
		t.repos[repo] = p
	}
	p.n++
	return func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		p.n--
		if p.n == 0 {
			close(p.done)
			delete(t.repos, repo)
		}
	}
}

// wait returns once no blob push to repo is in flight, or ctx is done.
func (t *blobPushTracker) wait(ctx context.Context, repo string) {
	t.mu.Lock()
	p, ok := t.repos[repo]
	t.mu.Unlock()
	if !ok {
		return
	}
	select {
	case <-p.done:
	case <-ctx.Done():
	}
}

// pushUpstream copies the image tree under desc to the upstream registry,
// fetching any content that is missing locally from this registry or from
// upstream first. It waits for the repository's blob pushes in flight, and
// all attempts share the UpstreamTimeout budget.
func (r *Registry) pushUpstream(ctx context.Context, repo, ref string, desc ocispec.Descriptor, cs content.Store, pushHeaders http.Header) error {
	log := r.log.WithName("upstreamPush").WithValues("ref", ref, "desc", desc)
	log.Info("Starting upstream image push")
	start := time.Now()

	if r.push.UpstreamTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.push.UpstreamTimeout)
		defer cancel()
	}
	r.blobPushes.wait(ctx, repo)

	err := retryWithBackoff(ctx, r.push.UpstreamRetries, func() error {
		rOpts := docker.ResolverOptions{Hosts: r.registryHosts, Headers: pushHeaders}
		fetcher, err := docker.NewResolver(rOpts).Fetcher(ctx, ref)
		if err != nil {
			return fmt.Errorf("failed to get fetcher: %w", err)
		}
		err = images.Dispatch(ctx, images.Handlers(images.ChildrenHandler(cs), remotes.FetchHandler(cs, fetcher)), nil, desc)
		if err != nil {
			return fmt.Errorf("failed to fetch image layers: %w", err)
		}

		pusher, err := docker.NewResolver(docker.ResolverOptions{Headers: pushHeaders}).Pusher(ctx, ref)
		if err != nil {
			return fmt.Errorf("failed to get pusher: %w", err)
		}
		if err = remotes.PushContent(ctx, pusher, desc, cs, nil, nil, nil); err != nil {
			return fmt.Errorf("failed to push image upstream: %w", err)
		}
		return nil
	}, log)
	if err != nil {
		log.Error(err, "Failed to push upstream image", "duration", time.Since(start))
		return err
	}
	log.Info("Finished upstream image push", "duration", time.Since(start))
	return nil
}

// retryWithBackoff calls fn up to attempts times, sleeping 2^n seconds between
// attempts, and gives up early once ctx is done.
func retryWithBackoff(ctx context.Context, attempts int, fn func() error, log logr.Logger) error {
	attempts = max(attempts, 1)
	var err error
	for i := range attempts {
		if err = fn(); err == nil {
			return nil
		}
		if i == attempts-1 {
			break
		}
		log.Error(err, "attempt failed", "attempt", i+1)
		select {
		case <-ctx.Done():
			return fmt.Errorf("gave up after %d attempts: %w", i+1, errors.Join(err, context.Cause(ctx)))
		case <-time.After(time.Duration(1<<uint(i)) * time.Second):
		}
	}
	return fmt.Errorf("failed after %d attempts: %w", attempts, err)
}

// Broadcast content for immediate discovery.
func (r *Registry) advertise(dgst digest.Digest) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := r.router.Advertise(ctx, []string{dgst.String()}, true)
	if err != nil {
		r.log.Error(err, "failed to advertise content")
	}
}

func (r *Registry) registryHosts(host string) ([]docker.RegistryHost, error) {
	return []docker.RegistryHost{{
		Host:         r.addr,
		Scheme:       "http",
		Path:         "/v2",
		Capabilities: docker.HostCapabilityPull | docker.HostCapabilityResolve,
	}, {
		Host:         host,
		Scheme:       "https",
		Path:         "/v2",
		Capabilities: docker.HostCapabilityPull | docker.HostCapabilityResolve,
	}}, nil
}
