// Package prometheus exposes a metrics collector's snapshot in the Prometheus
// text format, for a server that scrapes.
//
// A projection of what effect-golang-observe's metrics already keep, and
// nothing more: counts become counters, and durations and delays become
// histograms, because the collector's buckets are already cumulative -- which
// is what a Prometheus histogram is. The labels are the collector's own,
// bounded by construction: an operation the program declared, the runtime's
// event kind and its status. Nothing here can add a series the collector did
// not have.
package prometheus

import (
	"bytes"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mbauer83/effect-golang-observe/metrics"
)

// ContentType is the text exposition format's media type.
const ContentType = "text/plain; version=0.0.4; charset=utf-8"

// Exposition is how a snapshot is named when it is exposed.
type Exposition struct {
	// Namespace prefixes every metric name: "auth" exposes auth_runtime_events_total.
	Namespace string
	// Labels are added to every series: the service, the environment, what a
	// deployment's dashboards select by. Sorted when rendered.
	Labels map[string]string
}

// Render is a snapshot in the text exposition format.
func (exposition Exposition) Render(snapshot metrics.Snapshot) []byte {
	var out bytes.Buffer
	constant := exposition.constantLabels()
	labels := snapshot.Labels()

	events := exposition.name("runtime_events_total")
	writeHeader(&out, events, "counter", "Runtime events, by operation, kind and status.")
	for _, label := range labels {
		if count, counted := snapshot.Counts[label]; counted {
			writeSample(&out, events, seriesLabels(constant, label, ""), strconv.FormatUint(count, 10))
		}
	}
	exposition.writeHistogram(&out, "runtime_duration_seconds", "How long the work behind an event took.",
		constant, labels, snapshot.Durations)
	exposition.writeHistogram(&out, "runtime_delay_seconds", "How long a scheduled retry or repetition waited.",
		constant, labels, snapshot.Delays)
	return out.Bytes()
}

func (exposition Exposition) writeHistogram(
	out *bytes.Buffer,
	suffix string,
	help string,
	constant string,
	labels []metrics.Label,
	distributions map[metrics.Label]metrics.Distribution,
) {
	name := exposition.name(suffix)
	writeHeader(out, name, "histogram", help)
	for _, label := range labels {
		distribution, measured := distributions[label]
		if !measured {
			continue
		}
		for _, bucket := range distribution.Buckets {
			writeSample(out, name+"_bucket", seriesLabels(constant, label, seconds(bucket.AtMost)),
				strconv.FormatUint(bucket.Count, 10))
		}
		// A measurement past the last bound is in Count and in no bucket,
		// which is exactly what +Inf is for.
		count := strconv.FormatUint(distribution.Count, 10)
		writeSample(out, name+"_bucket", seriesLabels(constant, label, "+Inf"), count)
		writeSample(out, name+"_sum", seriesLabels(constant, label, ""), seconds(distribution.Sum))
		writeSample(out, name+"_count", seriesLabels(constant, label, ""), count)
	}
}

func (exposition Exposition) name(suffix string) string {
	if exposition.Namespace == "" {
		return suffix
	}
	return exposition.Namespace + "_" + suffix
}

// constantLabels are the exposition's own labels, rendered once per scrape.
func (exposition Exposition) constantLabels() string {
	names := make([]string, 0, len(exposition.Labels))
	for name := range exposition.Labels {
		names = append(names, name)
	}
	slices.Sort(names)
	pairs := make([]string, 0, len(names))
	for _, name := range names {
		pairs = append(pairs, name+`="`+escape(exposition.Labels[name])+`"`)
	}
	return strings.Join(pairs, ",")
}

func seriesLabels(constant string, label metrics.Label, bound string) string {
	pairs := []string{
		`operation="` + escape(label.Operation) + `"`,
		`kind="` + escape(string(label.Kind)) + `"`,
		`status="` + escape(string(label.Status)) + `"`,
	}
	if constant != "" {
		pairs = append([]string{constant}, pairs...)
	}
	if bound != "" {
		pairs = append(pairs, `le="`+bound+`"`)
	}
	return strings.Join(pairs, ",")
}

func writeHeader(out *bytes.Buffer, name string, kind string, help string) {
	out.WriteString("# HELP " + name + " " + help + "\n# TYPE " + name + " " + kind + "\n")
}

func writeSample(out *bytes.Buffer, name string, labels string, value string) {
	out.WriteString(name + "{" + labels + "} " + value + "\n")
}

func seconds(duration time.Duration) string {
	return strconv.FormatFloat(duration.Seconds(), 'g', -1, 64)
}

// escape is a label value as the text format requires it: backslash, double
// quote and newline escaped.
var escape = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace

// Handler is the collector's snapshot, taken when a scrape arrives.
func Handler(collector *metrics.Collector, exposition Exposition) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", ContentType)
		_, _ = writer.Write(exposition.Render(collector.Snapshot()))
	})
}
