// Package otlp exports a runtime's spans to an OpenTelemetry collector, over
// OTLP/HTTP with JSON bodies.
//
// The runtime already emits a span's start and end as events; this observer
// pairs them and sends each finished span on, and nothing else. It is inline
// on the observed fiber, so everything it does there is bounded: open spans
// and finished ones each have a ceiling, and a span past either is dropped
// and counted rather than waited for. Delivery happens elsewhere, in batches,
// on the program's own scope -- so closing the scope sends what is left.
package otlp

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mbauer83/effect-golang/effect"
)

// Settings are where spans go and how they are sent.
type Settings struct {
	// Endpoint is the collector's traces address: http://collector:4318/v1/traces.
	Endpoint string
	// Service names the program in every trace: the resource's service.name.
	Service string
	// Resource are further attributes of the program: its version, its
	// environment, where it runs.
	Resource map[string]string
	// Client sends the batches; nil is a client with a ten-second timeout.
	Client *http.Client
	// Batch is how many spans one request carries, at most; 512 when zero.
	Batch int
	// Interval is how often the queue is sent; five seconds when zero.
	Interval time.Duration
	// Queue is how many finished spans wait to be sent, at most; 4096 when
	// zero. A span finished while the queue is full is dropped and counted.
	Queue int
	// Open is how many spans may be open at once, at most; 65536 when zero.
	Open int
}

// ErrNoEndpoint is settings with nowhere to send spans.
var ErrNoEndpoint = errors.New("otlp: an exporter is told where the collector is")

func (settings Settings) withDefaults() (Settings, error) {
	if settings.Endpoint == "" {
		return Settings{}, ErrNoEndpoint
	}
	settings.Client = orDefault(settings.Client, &http.Client{Timeout: 10 * time.Second})
	settings.Batch = orDefault(settings.Batch, 512)
	settings.Interval = orDefault(settings.Interval, 5*time.Second)
	settings.Queue = orDefault(settings.Queue, 4096)
	settings.Open = orDefault(settings.Open, 65536)
	return settings, nil
}

func orDefault[A comparable](value A, fallback A) A {
	var zero A
	if value == zero {
		return fallback
	}
	return value
}

// Exporter is the observer that pairs span events into spans, and the queue
// they wait in.
type Exporter struct {
	settings Settings
	salt     uint64
	mu       sync.Mutex
	open     map[uint64]openSpan
	finished chan span
	dropped  atomic.Uint64
	sent     atomic.Uint64
}

// openSpan is a span started and not yet ended.
type openSpan struct {
	trace [16]byte
	// parent is the span this one was started within, when that span was
	// being exported too; zero for the root of a trace.
	parent     uint64
	name       string
	start      time.Time
	attributes []slog.Attr
}

// NewExporter is an exporter under these settings, or a refusal of settings
// with nowhere to send.
func NewExporter(settings Settings) (*Exporter, error) {
	settings, err := settings.withDefaults()
	if err != nil {
		return nil, err
	}
	var salt [8]byte
	_, _ = rand.Read(salt[:])
	return &Exporter{
		settings: settings,
		salt:     binary.BigEndian.Uint64(salt[:]) | 1,
		open:     make(map[uint64]openSpan),
		finished: make(chan span, settings.Queue),
	}, nil
}

// Observe records a span's start and sends its end on, and ignores every
// other event.
func (exporter *Exporter) Observe(_ context.Context, event effect.RuntimeEvent) {
	switch event.Kind {
	case effect.EventSpanStarted:
		exporter.start(event)
	case effect.EventSpanEnded:
		exporter.end(event)
	}
}

func (exporter *Exporter) start(event effect.RuntimeEvent) {
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	if len(exporter.open) >= exporter.settings.Open {
		exporter.dropped.Add(1)
		return
	}
	enclosing, isChild := exporter.open[event.ParentID]
	traceID, parent := enclosing.trace, event.ParentID
	if !isChild {
		_, _ = rand.Read(traceID[:])
		parent = 0
	}
	exporter.open[event.SpanID] = openSpan{
		trace: traceID, parent: parent, name: event.Operation,
		start: event.Timestamp, attributes: event.Attributes,
	}
}

func (exporter *Exporter) end(event effect.RuntimeEvent) {
	exporter.mu.Lock()
	started, isOpen := exporter.open[event.SpanID]
	delete(exporter.open, event.SpanID)
	exporter.mu.Unlock()
	if !isOpen {
		return
	}
	finished := span{open: started, id: event.SpanID, end: event.Timestamp, status: event.Status}
	select {
	case exporter.finished <- finished:
	default:
		exporter.dropped.Add(1)
	}
}

// Dropped is how many spans were not exported: past a ceiling, or in a batch
// the collector did not accept.
func (exporter *Exporter) Dropped() uint64 { return exporter.dropped.Load() }

// Sent is how many spans the collector accepted.
func (exporter *Exporter) Sent() uint64 { return exporter.sent.Load() }
