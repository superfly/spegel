package state

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"

	"github.com/spegel-org/spegel/internal/channel"
	"github.com/spegel-org/spegel/pkg/metrics"
	"github.com/spegel-org/spegel/pkg/oci"
	"github.com/spegel-org/spegel/pkg/routing"
)

func Track(ctx context.Context, ociStore oci.Store, router routing.Router, resolveLatestTag, brdcst bool) error {
	log := logr.FromContextOrDiscard(ctx)
	eventCh, err := ociStore.Subscribe(ctx)
	if err != nil {
		return err
	}
	if brdcst {
		// State tracking and the DHT start concurrently. Give bootstrap time
		// to find peers before attempting to publish the startup inventory.
		err := waitForRouter(ctx, router)
		if err != nil && ctx.Err() == nil {
			log.Error(err, "DHT did not become ready before the startup scan")
		}
	}
	immediateCh := make(chan time.Time, 1)
	immediateCh <- time.Now()
	close(immediateCh)
	expirationTicker := time.NewTicker(routing.KeyTTL - time.Minute)
	defer expirationTicker.Stop()
	tickerCh := channel.Merge(immediateCh, expirationTicker.C)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tickerCh:
			log.Info("running state update")
			err := tick(ctx, ociStore, router, resolveLatestTag, brdcst)
			if err != nil {
				log.Error(err, "received errors when updating all images")
				continue
			}
		case event, ok := <-eventCh:
			if !ok {
				return errors.New("event channel closed")
			}
			log.Info("OCI event", "key", event.Key, "type", event.Type)
			err := handle(ctx, router, event, brdcst)
			if err != nil {
				log.Error(err, "could not handle event")
				continue
			}
		}
	}
}

func waitForRouter(ctx context.Context, router routing.Router) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		ready, err := router.Ready(ctx)
		if ready || err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func tick(ctx context.Context, ociStore oci.Store, router routing.Router, resolveLatest, brdcst bool) error {
	advertisedImages := map[string]float64{}
	advertisedImageDigests := map[string]float64{}
	advertisedImageTags := map[string]float64{}
	advertisedKeys := map[string]float64{}
	var published, failed int
	var firstErr error
	advertise := func(key string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := router.Advertise(ctx, []string{key}, brdcst)
		if !brdcst {
			return err
		}
		if err != nil {
			failed++
			if firstErr == nil {
				firstErr = fmt.Errorf("%s: %w", key, err)
			}
		} else {
			published++
		}
		return nil
	}

	imgs, err := ociStore.ListImages(ctx)
	if err != nil {
		return err
	}
	for _, img := range imgs {
		advertisedImages[img.Registry] += 1
		advertisedImageDigests[img.Registry] += 1
		if !resolveLatest && img.IsLatestTag() {
			continue
		}
		tagName, ok := img.TagName()
		if !ok {
			continue
		}
		err := advertise(tagName)
		if err != nil {
			return err
		}
		advertisedImageTags[img.Registry] += 1
		advertisedKeys[img.Registry] += 1
	}

	contents, err := ociStore.ListContents(ctx)
	if err != nil {
		return err
	}
	for _, content := range contents {
		err := advertise(content.Digest.String())
		if err != nil {
			return err
		}
		for _, registry := range content.Registires {
			advertisedKeys[registry] += 1
		}
	}

	for k, v := range advertisedImages {
		metrics.AdvertisedImages.WithLabelValues(k).Set(v)
	}
	for k, v := range advertisedImageDigests {
		metrics.AdvertisedImageDigests.WithLabelValues(k).Set(v)
	}
	for k, v := range advertisedImageTags {
		metrics.AdvertisedImageTags.WithLabelValues(k).Set(v)
	}
	for k, v := range advertisedKeys {
		metrics.AdvertisedKeys.WithLabelValues(k).Set(v)
	}
	if brdcst {
		logr.FromContextOrDiscard(ctx).Info("finished publishing scanned keys", "published", published, "failed", failed)
	}
	if firstErr != nil {
		return fmt.Errorf("failed to publish %d scanned keys (first failure: %w)", failed, firstErr)
	}
	return nil
}

func handle(ctx context.Context, router routing.Router, event oci.OCIEvent, brdcst bool) error {
	if event.Type != oci.CreateEvent {
		return nil
	}
	err := router.Advertise(ctx, []string{event.Key}, brdcst)
	if err != nil {
		return err
	}
	return nil
}
