package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/spegel-org/spegel/pkg/httpx"
)

var (
	// DefaultRegisterer and DefaultGatherer are the implementations of the
	// prometheus Registerer and Gatherer interfaces that all metrics operations
	// will use. They are variables so that packages that embed this library can
	// replace them at runtime, instead of having to pass around specific
	// registries.
	DefaultRegisterer = prometheus.DefaultRegisterer
	DefaultGatherer   = prometheus.DefaultGatherer
)

var (
	MirrorRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "spegel_mirror_requests_total",
		Help: "Total number of mirror requests.",
	}, []string{"registry", "cache"})
	ResolveDurHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "spegel_resolve_duration_seconds",
		Help: "The duration for router to resolve a peer.",
	}, []string{"router"})
	AdvertisedImages = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "spegel_advertised_images",
		Help: "Number of images advertised to be available.",
	}, []string{"registry"})
	AdvertisedImageTags = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "spegel_advertised_image_tags",
		Help: "Number of image tags advertised to be available.",
	}, []string{"registry"})
	AdvertisedImageDigests = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "spegel_advertised_image_digests",
		Help: "Number of image digests advertised to be available.",
	}, []string{"registry"})
	AdvertisedKeys = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "spegel_advertised_keys",
		Help: "Number of keys advertised to be available.",
	}, []string{"registry"})
	UpstreamPushTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "spegel_upstream_push_total",
		Help: "Total number of pushed images forwarded to the upstream registry, by final result after retries.",
	}, []string{"result"})
	UpstreamPushDurHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "spegel_upstream_push_duration_seconds",
		Help:    "The duration of forwarding a pushed image to the upstream registry, including retries.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12),
	}, []string{"result"})
	UpstreamPushInFlight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "spegel_upstream_push_in_flight",
		Help: "Number of pushed images currently being forwarded to the upstream registry.",
	})
	UpstreamPushResumedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "spegel_upstream_push_resumed_total",
		Help: "Total number of upstream pushes found unfinished at startup and resumed.",
	})
)

func Register() {
	DefaultRegisterer.MustRegister(MirrorRequestsTotal)
	DefaultRegisterer.MustRegister(ResolveDurHistogram)
	DefaultRegisterer.MustRegister(AdvertisedImages)
	DefaultRegisterer.MustRegister(AdvertisedImageTags)
	DefaultRegisterer.MustRegister(AdvertisedImageDigests)
	DefaultRegisterer.MustRegister(AdvertisedKeys)
	DefaultRegisterer.MustRegister(UpstreamPushTotal)
	DefaultRegisterer.MustRegister(UpstreamPushDurHistogram)
	DefaultRegisterer.MustRegister(UpstreamPushInFlight)
	DefaultRegisterer.MustRegister(UpstreamPushResumedTotal)
	httpx.RegisterMetrics(DefaultRegisterer)
}
