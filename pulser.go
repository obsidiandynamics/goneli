package goneli

import (
	"context"

	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/obsidiandynamics/libstdgo/concurrent"
)

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

func (p *pulser) Close() {
	p.cancel()
}

func (p *pulser) Error() error {
	if err := p.err.Get(); err != nil {
		return err.(error)
	}
	return nil
}

func (p *pulser) Await() error {
	<-p.exited
	return p.Error()
}

type OnLeader func()

func Pulse(neli Neli, onLeader OnLeader) (Pulser, error) {
	err := validation.Errors{
		"neli":     validation.Validate(neli, validation.Required),
		"onLeader": validation.Validate(onLeader, validation.Required),
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
				onLeader()
			}
		}
	}()

	return p, nil
}
