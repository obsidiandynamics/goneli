package goneli

import (
	"errors"
	"time"

	"github.com/obsidiandynamics/libstdgo/concurrent"
)

var errPerformWithNoError = errors.New("")

func void(f func()) func() error {
	return func() error {
		f()
		return nil
	}
}

func performTimed(f func() error, timeout time.Duration) (bool, error) {
	errorRef := concurrent.NewAtomicReference()
	go func() {
		err := f()
		if err != nil {
			errorRef.Set(err)
		} else {
			errorRef.Set(errPerformWithNoError)
		}
	}()

	res := errorRef.Await(concurrent.RefNot(concurrent.RefNil()), timeout)
	if res == nil {
		return false, nil
	}
	if err := res.(error); err != errPerformWithNoError {
		return true, err
	}
	return true, nil
}
