package unit

// The exposition, read back by Prometheus's own parser: "this looks like the
// text format to me" is not a test.

import (
	"strings"
	"testing"

	"github.com/mbauer83/effect-golang-observe/metrics"
	"github.com/mbauer83/effect-golang/effect"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/mbauer83/effect-golang-observe-export/prometheus"
)

func observedWork(t *testing.T, observer effect.Observer, names ...string) {
	t.Helper()
	runtime, err := effect.NewRuntime(effect.WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		work := effect.Succeed[effect.Unit, effect.Never](effect.Unit{}).WithSpan(name)
		if exit := runtime.Run(t.Context(), effect.Unit{}, work); !exit.IsSuccess() {
			t.Fatal("the observed work failed")
		}
	}
	if cause := runtime.Close(t.Context()); !cause.IsEmpty() {
		t.Fatal(cause.String())
	}
}

func TestAnExpositionIsReadByPrometheusAsTheCollectorCountedIt(t *testing.T) {
	collector := metrics.NewCollector(metrics.NewVocabulary(`GET /status`))
	observedWork(t, collector, "GET /status", "GET /status", "unnamed")

	rendered := prometheus.Exposition{
		Namespace: "auth",
		Labels:    map[string]string{"service": `up2parts "auth"`},
	}.Render(collector.Snapshot())
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(string(rendered)))
	if err != nil {
		t.Fatalf("the exposition does not parse: %v\n%s", err, rendered)
	}

	durations, found := families["auth_runtime_duration_seconds"]
	if !found {
		t.Fatalf("expected a duration histogram, got %v", families)
	}
	var statusSpans uint64
	for _, series := range durations.GetMetric() {
		labels := map[string]string{}
		for _, pair := range series.GetLabel() {
			labels[pair.GetName()] = pair.GetValue()
		}
		if labels["service"] != `up2parts "auth"` {
			t.Fatalf("expected the constant label escaped and read back, got %q", labels["service"])
		}
		if labels["operation"] == "GET /status" && labels["kind"] == string(effect.EventSpanEnded) {
			statusSpans = series.GetHistogram().GetSampleCount()
		}
	}
	if statusSpans != 2 {
		t.Fatalf("expected the two GET /status spans in their own series, got %d", statusSpans)
	}
	if _, found := families["auth_runtime_events_total"]; !found {
		t.Fatal("expected the event counter")
	}
}
