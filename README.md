# effect-golang-observe-export

Exporting what [effect-golang-observe](https://github.com/mbauer83/effect-golang-observe)
reads out of a running [effect-golang](https://github.com/mbauer83/effect-golang)
program to the two ecosystems most deployments already run: an OpenTelemetry
collector for traces, and a Prometheus server for metrics.

Projections, not trackers. The spans are the runtime's own span events, paired;
the metrics are a `metrics.Collector`'s snapshot, rendered. Nothing here keeps a
second account of what the program did, and nothing here depends on anybody's
client library: both protocols are spoken with the standard library.

| Package | What it does |
| --- | --- |
| `otlp` | An observer that pairs span start and end events into spans and sends them in batches over OTLP/HTTP with JSON bodies. |
| `prometheus` | A collector's snapshot in the Prometheus text format, as an `http.Handler` or as a route on an effect-golang-web surface. |

## Traces

```go
exporter, err := otlp.NewExporter(otlp.Settings{
    Endpoint: "http://collector:4318/v1/traces",
    Service:  "checkout",
})
runtime, err := effect.NewRuntime(effect.WithObserver(exporter))

// In the program's scope: send every interval, and what is left when it closes.
effect.Scoped(func(scope effect.Scope) effect.Effect[R, E, A] {
    return effect.WidenError[E](otlp.Deliver[R](scope, exporter)).AndThen(serve(scope))
})
```

The observer runs inline on the observed fiber, so what it does there is bounded:
open spans and finished ones each have a ceiling, and a span past either is
dropped and counted (`Dropped`) rather than waited for. A batch the collector does
not accept is dropped and counted too, not retried: a program that held its
telemetry back to retry would be spending its memory on it.

A root span begins a trace with a random trace identifier and its children inherit
it. The runtime numbers spans per process, so span identifiers carry a random salt
per exporter. Span names are the runtime's: on an effect-golang-web surface,
`web.RouteSpans` makes every request a span named for its route.

## Metrics

```go
collector := metrics.NewCollector(metrics.NewVocabulary(names...))
runtime, err := effect.NewRuntime(effect.WithObserver(collector))

routes = append(routes, prometheus.Route[R, E]("/metrics", collector,
    prometheus.Exposition{Namespace: "checkout", Labels: map[string]string{"service": "checkout"}}))
```

Counts become `<namespace>_runtime_events_total`; durations and delays become
`_runtime_duration_seconds` and `_runtime_delay_seconds` histograms, because the
collector's buckets are already cumulative. The labels are the collector's own --
the operation, the event kind, the status -- so the number of series is fixed
before the program runs, and nothing here can add one.

## Tests

The projections are checked by parsers that have never seen this module: the
exposition by Prometheus's `expfmt`, and the batches by a collector decoding them
with the reference OTLP protobuf definitions. Both are test-only dependencies.
