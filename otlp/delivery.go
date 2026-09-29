package otlp

// Sending finished spans to the collector.

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/mbauer83/effect-golang/effect"
)

// Deliver sends finished spans for as long as the scope lives, every interval,
// and sends what is left when the scope closes.
//
// A batch the collector does not accept is dropped and counted, not retried:
// spans are evidence for somebody reading a trace later, and a program that
// held them back to retry would be spending its memory on its own telemetry.
func Deliver[R any](scope effect.Scope, exporter *Exporter) effect.Effect[R, effect.Never, effect.Unit] {
	operations := effect.For[R, effect.Never]()
	sending := operations.Suspend(func() effect.Effect[R, effect.Never, effect.Unit] {
		return effect.From(func(ctx context.Context, _ R) effect.Exit[effect.Never, effect.Unit] {
			exporter.drain(ctx, false)
			return effect.ExitSuccess[effect.Never](effect.Unit{})
		})
	}).Repeat(effect.Spaced[effect.Unit](exporter.settings.Interval))
	// Held as a resource of the scope, so its release -- the last send -- runs
	// when the scope closes, with a context the closing cannot cancel.
	return scope.AcquireRelease(scope.Fork(sending),
		func(effect.Fiber[effect.Never, uint64]) effect.Effect[R, effect.Never, effect.Unit] {
			return effect.AddFinalizer[R](func(ctx context.Context) error {
				exporter.drain(ctx, true)
				return nil
			})
		}).As(effect.Unit{})
}

// drain sends a batch of what is queued -- or all of it, when the program is
// closing.
func (exporter *Exporter) drain(ctx context.Context, all bool) {
	for {
		batch := exporter.take()
		if len(batch) == 0 {
			return
		}
		if err := exporter.send(ctx, batch); err != nil {
			exporter.dropped.Add(uint64(len(batch)))
		} else {
			exporter.sent.Add(uint64(len(batch)))
		}
		if !all {
			return
		}
	}
}

// take is up to a batch of queued spans, without waiting for more.
func (exporter *Exporter) take() []span {
	batch := make([]span, 0, exporter.settings.Batch)
	for len(batch) < exporter.settings.Batch {
		select {
		case finished := <-exporter.finished:
			batch = append(batch, finished)
		default:
			return batch
		}
	}
	return batch
}

func (exporter *Exporter) send(ctx context.Context, batch []span) error {
	body, err := exporter.encode(batch)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, exporter.settings.Endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := exporter.settings.Client.Do(request)
	if err != nil {
		return err
	}
	_ = response.Body.Close()
	if response.StatusCode/100 != 2 {
		return fmt.Errorf("otlp: the collector answered %d", response.StatusCode)
	}
	return nil
}
