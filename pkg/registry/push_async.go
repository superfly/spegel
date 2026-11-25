package registry

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/containerd/v2/core/remotes/docker"
	"github.com/containerd/errdefs"
	"github.com/go-logr/logr"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/spegel-org/spegel/pkg/metrics"
)

// pendingUpstreamPushLabel marks an image whose upstream push has not finished.
// Its value is the digest the push was started for, so a later push of the same
// tag is not cleared by an earlier one finishing.
const pendingUpstreamPushLabel = "spegel.org/upstream-push-pending"

// ResumeUpstreamPushes restarts upstream pushes that were still pending when
// the process last stopped. Headers from the original push request are not
// persisted, so resumed pushes carry only the configured upstream headers.
func (r *Registry) ResumeUpstreamPushes(ctx context.Context) error {
	if r.push.mode() == upstreamPushDisabled {
		return nil
	}
	cdc, err := r.getContainerdClient()
	if err != nil {
		return err
	}
	imgs, err := cdc.ImageService().List(ctx, fmt.Sprintf("labels.%q", pendingUpstreamPushLabel))
	if err != nil {
		return fmt.Errorf("failed to list images pending upstream push: %w", err)
	}
	pushHeaders := http.Header{}
	for k, v := range r.push.UpstreamHeaders {
		pushHeaders.Set(k, v)
	}
	cs := cdc.ContentStore()
	for _, img := range imgs {
		_, release := r.tryLockManifestPush(img.Name)
		if release == nil {
			continue
		}
		r.log.Info("Resuming upstream image push", "ref", img.Name, "digest", img.Target.Digest)
		metrics.UpstreamPushResumedTotal.Inc()
		go func() {
			defer release()
			r.pushUpstream(img.Name, img.Target, cs, pushHeaders)
		}()
	}
	return nil
}

func (r *Registry) pushUpstream(ref string, desc ocispec.Descriptor, cs content.Store, pushHeaders http.Header) {
	log := r.log.WithName("backgroundPush").WithValues("ref", ref, "desc", desc)
	log.Info("Starting upstream image push")
	ctx := context.Background()

	metrics.UpstreamPushInFlight.Inc()
	defer metrics.UpstreamPushInFlight.Dec()
	start := time.Now()

	err := retryWithBackoff(r.push.UpstreamRetries, func() error {
		ctx, cancel := context.WithTimeout(ctx, r.push.UpstreamTimeout)
		defer cancel()

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

	result := "success"
	if err != nil {
		result = "failure"
		log.Error(err, "Failed to push upstream image")
	} else {
		log.Info("Finished upstream image push")
	}
	metrics.UpstreamPushTotal.WithLabelValues(result).Inc()
	metrics.UpstreamPushDurHistogram.WithLabelValues(result).Observe(time.Since(start).Seconds())

	// A push that ran to completion, successfully or not, is no longer
	// pending. Failures are reported through the log and metrics above rather
	// than retried on every restart.
	if err := r.clearPendingUpstreamPush(ctx, ref, desc.Digest); err != nil {
		log.Error(err, "failed to clear pending upstream push label")
	}
}

// clearPendingUpstreamPush removes the pending label from ref, unless the label
// now records a different digest because the tag was pushed again meanwhile.
func (r *Registry) clearPendingUpstreamPush(ctx context.Context, ref string, dgst digest.Digest) error {
	cdc, err := r.getContainerdClient()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	is := cdc.ImageService()
	img, err := is.Get(ctx, ref)
	if errdefs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if img.Labels[pendingUpstreamPushLabel] != dgst.String() {
		return nil
	}
	delete(img.Labels, pendingUpstreamPushLabel)
	_, err = is.Update(ctx, img, "labels."+pendingUpstreamPushLabel)
	if errdefs.IsNotFound(err) {
		return nil
	}
	return err
}

// Simple exponential backoff with 2^n second delay between attempts.
func retryWithBackoff(attempts int, fn func() error, log logr.Logger) error {
	var err error
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		} else if i < attempts-1 {
			log.Error(err, "attempt failed", "attempt", i+1)
			time.Sleep(time.Duration(1<<uint(i)) * time.Second)
		}
	}
	return fmt.Errorf("failed after %d attempts: %w", attempts, err)
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
