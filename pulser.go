package goneli

import (
	"context"

	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/obsidiandynamics/libstdgo/concurrent"
)

// Pulser performs continuous pulsing of a Neli instance from a dedicated background Goroutine.
type Pulser interface {
	Close()
	Error() error
	Await() error
}

type pulser struct {
	cancel func()
	err    concurrent.AtomicReference
	exited chan int
}

// Close the pulser, terminating the underlying Goroutine.
func (p *pulser) Close() {
	p.cancel()
}

// Error returns an error that has been accumulated as a result of a failed pulse call. Nil is returned
// if no error occurred.
func (p *pulser) Error() error {
	if err := p.err.Get(); err != nil {
		return err.(error)
	}
	return nil
}

// Await the termination of the pulser Goroutine, returning an accumulated error (if applicable).
func (p *pulser) Await() error {
	<-p.exited
	return p.Error()
}

// LeaderTask is an activity performed by the client application if it is the elected leader. The task should
// perform a small amount of work that is exclusively attributable to a leader, and return immediately. For as
// long as the associated Neli instance is the leader, the task will be invoked repeatedly; therefore, it should
// break down any long-running work into bite-sized chunks that can be safely performed without causing excessive
// blocking.
type LeaderTask func()

func pulse(neli Neli, task LeaderTask) (Pulser, error) {
	err := validation.Errors{
		"neli": validation.Validate(neli, validation.Required),
		"task": validation.Validate(task, validation.Required),
	}.Filter()
	if err != nil {
		return nil, err
	}

	ctx, cancel := concurrent.Timeout(context.Background(), concurrent.Indefinitely)
	p := &pulser{
		cancel: cancel,
		err:    concurrent.NewAtomicReference(),
		exited: make(chan int),
	}

	go func() {
		defer close(p.exited)

		for {
			// Pulse our presence, allowing for some time to acquire leader status.
			// Will return instantly if already leader.
			isLeader, err := neli.PulseCtx(ctx)
			if err == ErrNonLivePulse {
				return
			}

			if ctx.Err() == context.Canceled {
				return
			}

			if err != nil {
				p.err.Set(err)
				return
			}

			// We hold leader status, invoke the callback.
			if isLeader {
				task()
			}
		}
	}()

	return p, nil
}
