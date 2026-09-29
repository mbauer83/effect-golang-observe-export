package unit

// Spans, read back by a collector that decodes with the reference OTLP
// definitions: what arrives is what the protocol says a span is.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/mbauer83/effect-golang/effect"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/mbauer83/effect-golang-observe-export/otlp"
)

// collector is an OTLP/HTTP endpoint that keeps what it was sent.
type collector struct {
	mu      sync.Mutex
	spans   []*tracev1.Span
	service string
}

func newCollector(t *testing.T) (*collector, *httptest.Server) {
	t.Helper()
	received := &collector{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		var export collectortrace.ExportTraceServiceRequest
		if err := protojson.Unmarshal(body, &export); err != nil {
			t.Errorf("the batch is not OTLP JSON: %v\n%s", err, body)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		received.mu.Lock()
		defer received.mu.Unlock()
		for _, resource := range export.GetResourceSpans() {
			for _, attribute := range resource.GetResource().GetAttributes() {
				if attribute.GetKey() == "service.name" {
					received.service = attribute.GetValue().GetStringValue()
				}
			}
			for _, scope := range resource.GetScopeSpans() {
				received.spans = append(received.spans, scope.GetSpans()...)
			}
		}
	}))
	t.Cleanup(server.Close)
	return received, server
}

func TestSpansArriveAsOneTraceWithTheirParentage(t *testing.T) {
	received, server := newCollector(t)
	exporter, err := otlp.NewExporter(otlp.Settings{Endpoint: server.URL + "/v1/traces", Service: "auth"})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := effect.NewRuntime(effect.WithObserver(exporter))
	if err != nil {
		t.Fatal(err)
	}
	program := effect.Scoped(func(scope effect.Scope) effect.Effect[effect.Unit, effect.Never, effect.Unit] {
		inner := effect.Succeed[effect.Unit, effect.Never](effect.Unit{}).WithSpan("read the session")
		return otlp.Deliver[effect.Unit](scope, exporter).AndThen(inner.WithSpan("GET /status"))
	})
	if exit := runtime.Run(t.Context(), effect.Unit{}, program); !exit.IsSuccess() {
		t.Fatal("the program failed")
	}

	received.mu.Lock()
	defer received.mu.Unlock()
	if len(received.spans) != 2 || received.service != "auth" {
		t.Fatalf("expected two spans from auth, got %d from %q", len(received.spans), received.service)
	}
	byName := map[string]*tracev1.Span{}
	for _, span := range received.spans {
		byName[span.GetName()] = span
	}
	parent, child := byName["GET /status"], byName["read the session"]
	if parent == nil || child == nil {
		t.Fatalf("expected both spans by name, got %v", byName)
	}
	if string(child.GetTraceId()) != string(parent.GetTraceId()) || string(child.GetParentSpanId()) != string(parent.GetSpanId()) {
		t.Fatal("expected the inner span to be the outer one's child, in one trace")
	}
	if len(parent.GetParentSpanId()) != 0 || parent.GetStatus().GetCode() != tracev1.Status_STATUS_CODE_OK {
		t.Fatalf("expected a successful root, got %v", parent)
	}
	if parent.GetEndTimeUnixNano() < parent.GetStartTimeUnixNano() {
		t.Fatal("expected a span to end after it starts")
	}
}

func TestASpanFinishedIntoAFullQueueIsDroppedAndCounted(t *testing.T) {
	exporter, err := otlp.NewExporter(otlp.Settings{Endpoint: "http://127.0.0.1:1/v1/traces", Service: "auth", Queue: 1})
	if err != nil {
		t.Fatal(err)
	}
	observedWork(t, exporter, "one", "two", "three")
	if exporter.Dropped() != 2 {
		t.Fatalf("expected two of three spans dropped with a queue of one, got %d", exporter.Dropped())
	}
}
