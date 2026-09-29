package otlp

// A batch of spans as OTLP/HTTP's JSON encoding spells it.

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"time"

	"github.com/mbauer83/effect-golang/effect"
)

// span is a finished span, waiting to be sent.
type span struct {
	open   openSpan
	id     uint64
	end    time.Time
	status effect.EventStatus
}

type exportRequest struct {
	ResourceSpans []resourceSpans `json:"resourceSpans"`
}

type resourceSpans struct {
	Resource   resource     `json:"resource"`
	ScopeSpans []scopeSpans `json:"scopeSpans"`
}

type resource struct {
	Attributes []attribute `json:"attributes"`
}

type scopeSpans struct {
	Scope instrumentationScope `json:"scope"`
	Spans []wireSpan           `json:"spans"`
}

type instrumentationScope struct {
	Name string `json:"name"`
}

type wireSpan struct {
	TraceID      string      `json:"traceId"`
	SpanID       string      `json:"spanId"`
	ParentSpanID string      `json:"parentSpanId,omitempty"`
	Name         string      `json:"name"`
	Kind         int         `json:"kind"`
	Start        string      `json:"startTimeUnixNano"`
	End          string      `json:"endTimeUnixNano"`
	Attributes   []attribute `json:"attributes,omitempty"`
	Status       spanStatus  `json:"status"`
}

type spanStatus struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
}

type attribute struct {
	Key   string   `json:"key"`
	Value anyValue `json:"value"`
}

// anyValue is OTLP's AnyValue, in the three shapes a runtime attribute takes.
type anyValue struct {
	String *string  `json:"stringValue,omitempty"`
	Int    *string  `json:"intValue,omitempty"`
	Double *float64 `json:"doubleValue,omitempty"`
	Bool   *bool    `json:"boolValue,omitempty"`
}

// OTLP's span kind and status codes, as far as a runtime span has them.
const (
	kindInternal = 1
	statusOK     = 1
	statusError  = 2
)

func (exporter *Exporter) encode(batch []span) ([]byte, error) {
	spans := make([]wireSpan, 0, len(batch))
	for _, finished := range batch {
		spans = append(spans, exporter.wireSpanOf(finished))
	}
	return json.Marshal(exportRequest{ResourceSpans: []resourceSpans{{
		Resource:   resource{Attributes: exporter.resourceAttributes()},
		ScopeSpans: []scopeSpans{{Scope: instrumentationScope{Name: "effect-golang"}, Spans: spans}},
	}}})
}

func (exporter *Exporter) wireSpanOf(finished span) wireSpan {
	wire := wireSpan{
		TraceID:    hex.EncodeToString(finished.open.trace[:]),
		SpanID:     exporter.spanID(finished.id),
		Name:       finished.open.name,
		Kind:       kindInternal,
		Start:      strconv.FormatInt(finished.open.start.UnixNano(), 10),
		End:        strconv.FormatInt(finished.end.UnixNano(), 10),
		Attributes: attributesOf(finished.open.attributes),
		Status:     statusOf(finished.status),
	}
	if finished.open.parent != 0 {
		wire.ParentSpanID = exporter.spanID(finished.open.parent)
	}
	return wire
}

// spanID is a runtime span identifier as a collector-wide one: the runtime
// numbers spans per process, so each exporter mixes in a random salt of its
// own.
func (exporter *Exporter) spanID(id uint64) string {
	var bytes [8]byte
	binary.BigEndian.PutUint64(bytes[:], id^exporter.salt)
	return hex.EncodeToString(bytes[:])
}

func statusOf(status effect.EventStatus) spanStatus {
	switch status {
	case effect.EventStatusSuccess:
		return spanStatus{Code: statusOK}
	case effect.EventStatusNone:
		return spanStatus{}
	default:
		return spanStatus{Code: statusError, Message: string(status)}
	}
}

func (exporter *Exporter) resourceAttributes() []attribute {
	names := make([]string, 0, len(exporter.settings.Resource))
	for name := range exporter.settings.Resource {
		names = append(names, name)
	}
	slices.Sort(names)
	attributes := []attribute{textAttribute("service.name", exporter.settings.Service)}
	for _, name := range names {
		attributes = append(attributes, textAttribute(name, exporter.settings.Resource[name]))
	}
	return attributes
}

func textAttribute(key string, value string) attribute {
	return attribute{Key: key, Value: anyValue{String: &value}}
}

// attributesOf are a span's annotations as OTLP attributes: numbers and
// booleans as themselves, and every other kind as its text.
func attributesOf(attributes []slog.Attr) []attribute {
	converted := make([]attribute, 0, len(attributes))
	for _, attr := range attributes {
		converted = append(converted, attribute{Key: attr.Key, Value: valueOf(attr.Value.Resolve())})
	}
	return converted
}

func valueOf(value slog.Value) anyValue {
	switch value.Kind() {
	case slog.KindInt64:
		text := strconv.FormatInt(value.Int64(), 10)
		return anyValue{Int: &text}
	case slog.KindUint64:
		text := strconv.FormatUint(value.Uint64(), 10)
		return anyValue{Int: &text}
	case slog.KindFloat64:
		number := value.Float64()
		return anyValue{Double: &number}
	case slog.KindBool:
		flag := value.Bool()
		return anyValue{Bool: &flag}
	default:
		text := fmt.Sprint(value.Any())
		return anyValue{String: &text}
	}
}
