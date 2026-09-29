package prometheus

import (
	"net/http"

	"github.com/mbauer83/effect-golang-web/web"
	"github.com/mbauer83/effect-golang/effect"
	"github.com/mbauer83/effect-golang-observe/metrics"
)

// Route is the exposition as a route on an effect-golang-web surface, so a
// program's metrics are scraped from the surface it already serves.
func Route[R, E any](path string, collector *metrics.Collector, exposition Exposition) web.Route[R, E] {
	return web.Handle(
		web.GET(path, web.Nothing(), web.ReturnsRaw(http.StatusOK, ContentType)).
			WithSummary("This program's runtime metrics, for a Prometheus scrape"),
		func(effect.Unit) effect.Effect[R, E, []byte] {
			return effect.Succeed[R, E](exposition.Render(collector.Snapshot()))
		},
	)
}
